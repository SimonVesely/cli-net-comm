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
        event::{self, Event, KeyCode, KeyEventKind},
        execute,
        terminal::{
            Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode,
            enable_raw_mode,
        },
    },
    layout::{Alignment, Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, BorderType, Cell, Paragraph, Row, Table, Wrap},
};
use tokio::sync::{Notify, watch};
use utils::{relay::RelayStatus, *};

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

pub async fn run(mut app: App) -> io::Result<()> {
    let mut terminal = ratatui::init();
    let result = event_loop(&mut terminal, &mut app).await;
    ratatui::restore();
    result
}

async fn event_loop(terminal: &mut DefaultTerminal, app: &mut App) -> io::Result<()> {
    loop {
        terminal.draw(|frame| draw(frame, app))?;

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

        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
            KeyCode::Char('r') => app.scan_now.notify_one(),
            KeyCode::Char('d') => handle_pending_invite(&app.config, &app.invites, false).await,
            KeyCode::Char('1') => {
                suspend()?;
                edit_name(&mut app.config);
                let _ = app.config_tx.send(app.config.clone());
                resume(terminal)?;
            }
            KeyCode::Char('2') => {
                suspend()?;
                network_setup(&mut app.config);
                let _ = app.config_tx.send(app.config.clone());
                resume(terminal)?;
            }
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

fn draw(frame: &mut Frame, app: &App) {
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
    draw_help(frame, help, app.invites.peek().is_some());
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

fn draw_help(frame: &mut Frame, area: Rect, invite: bool) {
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
