use std::{
    io::{self, stdout},
    net::IpAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use ratatui::{
    DefaultTerminal, Frame, Terminal,
    backend::CrosstermBackend,
    crossterm::{
        cursor::{Hide, Show},
        event::{self, Event, KeyCode, KeyEvent, KeyEventKind},
        execute,
        terminal::{
            Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode,
            enable_raw_mode,
        },
    },
    layout::{Alignment, Constraint, Flex, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, BorderType, Cell, Clear as Wipe, Paragraph, Row, Table, Wrap},
};
use tokio::sync::{Notify, watch};
use utils::{relay::RelayStatus, *};

use crate::input::TextInput;

const ACCENT: Color = Color::Rgb(138, 92, 255);
const LAVENDER: Color = Color::Rgb(185, 166, 255);
const MUTED: Color = Color::Rgb(130, 130, 130);
const BORDER: Color = Color::Rgb(70, 70, 70);
const OK: Color = Color::Rgb(60, 207, 145);
const WARN: Color = Color::Rgb(240, 190, 80);
const ERR: Color = Color::Rgb(229, 72, 77);

const TICK: Duration = Duration::from_millis(200);

const BANNER: [&str; 3] = [
    "  ▜ ▘      ▗        ▌                 ▘    ▗",
    "▛▘▐ ▌▄▖▛▌█▌▜▘▌▌▌▛▌▛▘▙▘▄▖▛▘▛▌▛▛▌▛▛▌▌▌▛▌▌▛▘▀▌▜▘▛▌▛▘",
    "▙▖▐▖▌  ▌▌▙▖▐▖▚▚▘▙▌▌ ▛▖  ▙▖▙▌▌▌▌▌▌▌▙▌▌▌▌▙▖█▌▐▖▙▌▌",
];

pub struct App {
    pub config: Config,
    pub config_tx: watch::Sender<Config>,
    pub online: OnlineList,
    pub invites: Invites,
    pub relay_state: RelayState,
    pub scan_now: Arc<Notify>,
    pub listener_error: Arc<Mutex<Option<String>>>,
}

enum Popup {
    Name(NameForm),
    Network(NetworkForm),
}

struct NameForm {
    input: TextInput,
    error: Option<&'static str>,
}

impl NameForm {
    fn new(config: &Config) -> Self {
        let current = match config.username() {
            "EMPTY" => "",
            name => name,
        };
        Self {
            input: TextInput::new(current),
            error: None,
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum NetField {
    Interface,
    Port,
    Relay,
}

impl NetField {
    fn next(self) -> Self {
        match self {
            NetField::Interface => NetField::Port,
            NetField::Port => NetField::Relay,
            NetField::Relay => NetField::Interface,
        }
    }

    fn prev(self) -> Self {
        match self {
            NetField::Interface => NetField::Relay,
            NetField::Port => NetField::Interface,
            NetField::Relay => NetField::Port,
        }
    }
}

struct NetworkForm {
    iface: usize,
    port: TextInput,
    relay: TextInput,
    focus: NetField,
    error: Option<String>,
}

impl NetworkForm {
    fn new(config: &Config) -> Self {
        Self {
            iface: config
                .bind_options()
                .iter()
                .position(|ip| *ip == config.ip())
                .unwrap_or(0),
            port: TextInput::new(config.port().to_string()),
            relay: TextInput::new(config.relay().map(|r| r.to_string()).unwrap_or_default()),
            focus: NetField::Interface,
            error: None,
        }
    }

    fn apply(&self, config: &mut Config) -> Result<(), String> {
        let port = self
            .port
            .value()
            .trim()
            .parse::<u16>()
            .ok()
            .filter(|p| *p != 0)
            .ok_or("port must be a number from 1 to 65535")?;
        let relay = match self.relay.value().trim() {
            "" => None,
            url => Some(
                RelayAddr::parse(url)
                    .ok_or("relay must look like https://host or ws://host:port")?,
            ),
        };
        let ip = config.bind_options()[self.iface];
        config.set_ip(ip);
        config.set_port(port);
        config.set_relay(relay);
        Ok(())
    }
}

fn handle_popup(popup: &mut Popup, key: KeyEvent, app: &mut App) -> bool {
    if key.code == KeyCode::Esc {
        return true;
    }
    match popup {
        Popup::Name(form) => {
            if key.code == KeyCode::Enter {
                match app.config.set_username(form.input.value()) {
                    Ok(()) => {
                        let _ = app.config_tx.send(app.config.clone());
                        return true;
                    }
                    Err(e) => form.error = Some(e),
                }
            } else if form.input.handle_key(key) {
                form.error = None;
            }
        }
        Popup::Network(form) => match key.code {
            KeyCode::Enter => match form.apply(&mut app.config) {
                Ok(()) => {
                    let _ = app.config_tx.send(app.config.clone());
                    return true;
                }
                Err(e) => form.error = Some(e),
            },
            KeyCode::Tab | KeyCode::Down => form.focus = form.focus.next(),
            KeyCode::BackTab | KeyCode::Up => form.focus = form.focus.prev(),
            KeyCode::Left | KeyCode::Right if form.focus == NetField::Interface => {
                let count = app.config.bind_options().len();
                form.iface = if key.code == KeyCode::Right {
                    (form.iface + 1) % count
                } else {
                    (form.iface + count - 1) % count
                };
            }
            KeyCode::Char(c) if form.focus == NetField::Port && !c.is_ascii_digit() => {}
            _ => {
                let input = match form.focus {
                    NetField::Port => &mut form.port,
                    NetField::Relay => &mut form.relay,
                    NetField::Interface => return false,
                };
                if input.handle_key(key) {
                    form.error = None;
                }
            }
        },
    }
    false
}

pub async fn run(mut app: App) -> io::Result<()> {
    let mut terminal = ratatui::init();
    let result = event_loop(&mut terminal, &mut app).await;
    ratatui::restore();
    result
}

async fn event_loop(terminal: &mut DefaultTerminal, app: &mut App) -> io::Result<()> {
    let mut popup: Option<Popup> = None;
    loop {
        terminal.draw(|frame| draw(frame, app, popup.as_ref()))?;

        let polled = tokio::task::block_in_place(|| -> io::Result<Option<Event>> {
            if event::poll(TICK)? {
                event::read().map(Some)
            } else {
                Ok(None)
            }
        })?;
        let Some(Event::Key(key)) = polled else {
            continue;
        };
        if key.kind != KeyEventKind::Press {
            continue;
        }

        if let Some(open) = popup.as_mut() {
            if handle_popup(open, key, app) {
                popup = None;
            }
            continue;
        }

        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
            KeyCode::Char('r') => app.scan_now.notify_one(),
            KeyCode::Char('d') => handle_pending_invite(&app.config, &app.invites, false).await,
            KeyCode::Char('1') => popup = Some(Popup::Name(NameForm::new(&app.config))),
            KeyCode::Char('2') => popup = Some(Popup::Network(NetworkForm::new(&app.config))),
            KeyCode::Char('3') => {
                suspend()?;
                active_connection(&app.config, &app.online, &app.relay_state).await;
                resume(terminal)?;
            }
            KeyCode::Char('a') if app.invites.peek().is_some() => {
                suspend()?;
                handle_pending_invite(&app.config, &app.invites, true).await;
                resume(terminal)?;
            }
            _ => {}
        }
    }
}

fn suspend() -> io::Result<()> {
    disable_raw_mode()?;
    execute!(stdout(), LeaveAlternateScreen, Show)
}

fn resume(terminal: &mut DefaultTerminal) -> io::Result<()> {
    enable_raw_mode()?;
    execute!(stdout(), EnterAlternateScreen, Clear(ClearType::All), Hide)?;
    *terminal = Terminal::new(CrosstermBackend::new(stdout()))?;
    Ok(())
}

fn draw(frame: &mut Frame, app: &App, popup: Option<&Popup>) {
    let invite = app.invites.peek();
    let [header, body, invite_area, help] = Layout::vertical([
        Constraint::Length(5),
        Constraint::Min(6),
        Constraint::Length(if invite.is_some() { 3 } else { 0 }),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    draw_header(frame, header, app);

    let [lan, relay] =
        Layout::horizontal([Constraint::Percentage(55), Constraint::Percentage(45)]).areas(body);
    draw_lan(frame, lan, app);
    draw_relay(frame, relay, app);

    if let Some((from, origin)) = invite {
        draw_invite(frame, invite_area, &from, &origin);
    }
    match popup {
        Some(Popup::Name(form)) => draw_name_popup(frame, form),
        Some(Popup::Network(form)) => draw_network_popup(frame, form, app),
        None => {}
    }
    draw_help(frame, help, app.invites.peek().is_some(), popup);
}

fn centered(area: Rect, width: u16, height: u16) -> Rect {
    let [row] = Layout::vertical([Constraint::Length(height.min(area.height))])
        .flex(Flex::Center)
        .areas(area);
    let [rect] = Layout::horizontal([Constraint::Length(width.min(area.width))])
        .flex(Flex::Center)
        .areas(row);
    rect
}

fn popup_frame(frame: &mut Frame, area: Rect, title: &str) -> Rect {
    let block = Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(ACCENT))
        .title(Line::from(format!(" {title} ")).fg(LAVENDER).bold());
    let inner = block.inner(area);
    frame.render_widget(Wipe, area);
    frame.render_widget(block, area);
    Layout::horizontal([Constraint::Min(0)])
        .horizontal_margin(2)
        .vertical_margin(1)
        .split(inner)[0]
}

fn field(title: &str, focused: bool) -> Block<'static> {
    let color = if focused { LAVENDER } else { BORDER };
    Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(color))
        .title(Line::from(format!(" {title} ")).fg(if focused { LAVENDER } else { MUTED }))
}

fn error_line(error: Option<&str>) -> Paragraph<'static> {
    Paragraph::new(
        error
            .map(|e| Line::from(format!("✗ {e}")).fg(ERR))
            .unwrap_or_default(),
    )
}

fn draw_name_popup(frame: &mut Frame, form: &NameForm) {
    let area = centered(frame.area(), 56, 9);
    let inner = popup_frame(frame, area, "Your name");
    let [hint, input, error] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(3),
        Constraint::Length(1),
    ])
    .areas(inner);
    frame.render_widget(
        Paragraph::new("Others see this name on the LAN and on the relay.").fg(MUTED),
        hint,
    );
    form.input.render(frame, input, field("name", true), true);
    frame.render_widget(error_line(form.error), error);
}

fn draw_network_popup(frame: &mut Frame, form: &NetworkForm, app: &App) {
    let area = centered(frame.area(), 66, 15);
    let inner = popup_frame(frame, area, "Network setup");
    let [iface, port, relay, error] = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(3),
        Constraint::Length(3),
        Constraint::Length(1),
    ])
    .areas(inner);

    let ip = app.config.bind_options()[form.iface];
    let focused = form.focus == NetField::Interface;
    let arrow = |s: &'static str| {
        Span::from(s)
            .fg(if focused { ACCENT } else { BORDER })
            .bold()
    };
    let iface_line = Line::from(vec![
        arrow("◂ "),
        Span::from(ip.to_string()).bold(),
        Span::from(bind_label(ip)).fg(MUTED),
        arrow(" ▸"),
    ]);
    frame.render_widget(
        Paragraph::new(iface_line).block(field("listen on", focused)),
        iface,
    );
    form.port.render(
        frame,
        port,
        field("port", form.focus == NetField::Port),
        form.focus == NetField::Port,
    );
    form.relay.render(
        frame,
        relay,
        field("relay URL · empty = off", form.focus == NetField::Relay),
        form.focus == NetField::Relay,
    );
    frame.render_widget(error_line(form.error.as_deref()), error);
}

fn panel(title: String) -> Block<'static> {
    Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(BORDER))
        .title(Line::from(format!(" {title} ")).fg(LAVENDER).bold())
}

fn bind_label(ip: IpAddr) -> &'static str {
    if ip.is_unspecified() {
        " (all interfaces)"
    } else if ip.is_loopback() {
        " (loopback)"
    } else {
        ""
    }
}

fn draw_header(frame: &mut Frame, area: Rect, app: &App) {
    let block = Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(ACCENT));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let [logo, info] =
        Layout::horizontal([Constraint::Length(52), Constraint::Min(0)]).areas(inner);

    let banner: Vec<Line> = BANNER.iter().map(|l| Line::from(*l).fg(ACCENT)).collect();
    frame.render_widget(Paragraph::new(banner), logo);

    let cfg = &app.config;
    let label = |text: &'static str| Span::from(text).fg(MUTED);
    let mut lines = vec![
        Line::from(vec![
            label("name   "),
            Span::from(cfg.username().to_string()).bold(),
        ]),
        Line::from(vec![
            label("listen "),
            Span::from(format!(
                "{}:{}{}",
                cfg.ip(),
                cfg.port(),
                bind_label(cfg.ip())
            )),
        ]),
    ];
    if let Some(err) = app.listener_error.lock().unwrap().clone() {
        lines.push(Line::from(vec![label("error  "), Span::from(err).fg(ERR)]));
    } else {
        lines.push(Line::from(vec![
            label("relay  "),
            Span::from(cfg.relay().map_or("off".to_string(), |r| r.to_string())),
        ]));
    }
    frame.render_widget(Paragraph::new(lines).alignment(Alignment::Right), info);
}

fn draw_lan(frame: &mut Frame, area: Rect, app: &App) {
    let peers = app.online.snapshot();
    let block = panel(format!("LAN · {}", peers.len()));

    if peers.is_empty() {
        let text = Paragraph::new(vec![
            Line::default(),
            Line::from("nobody found yet").fg(MUTED),
            Line::from(vec![
                Span::from("press ").fg(MUTED),
                Span::from("r").fg(ACCENT).bold(),
                Span::from(" to scan").fg(MUTED),
            ]),
        ])
        .alignment(Alignment::Center)
        .block(block);
        frame.render_widget(text, area);
        return;
    }

    let rows = peers.iter().map(|(addr, peer)| {
        Row::new(vec![
            Cell::from(Line::from(vec![
                Span::from("● ").fg(OK),
                Span::from(peer.username.clone()).bold(),
            ])),
            Cell::from(addr.to_string()),
            Cell::from(format!("{}s ago", peer.last_seen.elapsed().as_secs())).fg(MUTED),
        ])
    });
    let table = Table::new(
        rows,
        [
            Constraint::Fill(2),
            Constraint::Length(22),
            Constraint::Length(9),
        ],
    )
    .header(
        Row::new(["name", "address", "seen"])
            .fg(MUTED)
            .bottom_margin(1),
    )
    .block(block);
    frame.render_widget(table, area);
}

fn draw_relay(frame: &mut Frame, area: Rect, app: &App) {
    let status = app.relay_state.status();
    let (dot, color) = match &status {
        RelayStatus::Online => ("●", OK),
        RelayStatus::Connecting => ("◌", WARN),
        RelayStatus::Error(_) => ("●", ERR),
        RelayStatus::Off => ("○", MUTED),
    };
    let users = app.relay_state.users();
    let block = panel(format!("Relay · {}", users.len()));

    let mut lines = vec![
        Line::from(vec![
            Span::from(format!("{dot} ")).fg(color),
            Span::from(status.to_string()).fg(MUTED),
        ]),
        Line::default(),
    ];
    if users.is_empty() && status == RelayStatus::Online {
        lines.push(Line::from("nobody else online").fg(MUTED));
    }
    lines.extend(users.iter().map(|name| {
        Line::from(vec![
            Span::from("● ").fg(OK),
            Span::from(name.clone()).bold(),
        ])
    }));
    frame.render_widget(
        Paragraph::new(lines).wrap(Wrap { trim: true }).block(block),
        area,
    );
}

fn draw_invite(frame: &mut Frame, area: Rect, from: &str, origin: &str) {
    let text = Line::from(vec![
        Span::from(" Chat invite from "),
        Span::from(from.to_string()).bold(),
        Span::from(format!(" ({origin})   ")),
        Span::from("a").bold().underlined(),
        Span::from(" accept   "),
        Span::from("d").bold().underlined(),
        Span::from(" decline "),
    ]);
    let block = Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(LAVENDER));
    frame.render_widget(
        Paragraph::new(text)
            .alignment(Alignment::Center)
            .style(Style::new().fg(LAVENDER).add_modifier(Modifier::BOLD))
            .block(block),
        area,
    );
}

fn draw_help(frame: &mut Frame, area: Rect, invite: bool, popup: Option<&Popup>) {
    let keys = match popup {
        Some(Popup::Name(_)) => vec![("Enter", "save"), ("Esc", "cancel")],
        Some(Popup::Network(_)) => vec![
            ("↑↓", "field"),
            ("←→", "interface"),
            ("Enter", "save"),
            ("Esc", "cancel"),
        ],
        None => {
            let mut keys = vec![
                ("1", "name"),
                ("2", "network"),
                ("3", "connect"),
                ("r", "rescan"),
            ];
            if invite {
                keys.extend([("a", "accept"), ("d", "decline")]);
            }
            keys.push(("q", "quit"));
            keys
        }
    };
    let spans: Vec<Span> = keys
        .into_iter()
        .flat_map(|(key, what)| {
            [
                Span::from(format!(" {key} "))
                    .fg(Color::Black)
                    .bg(LAVENDER)
                    .bold(),
                Span::from(format!(" {what}   ")).fg(MUTED),
            ]
        })
        .collect();
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}
