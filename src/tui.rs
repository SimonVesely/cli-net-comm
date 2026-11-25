use std::{
    io,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use ratatui::{
    DefaultTerminal, Frame,
    crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind},
    layout::{Alignment, Constraint, Flex, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, BorderType, Cell, Clear as Wipe, Paragraph, Row, Table, Wrap},
};
use tokio::{
    sync::{Notify, mpsc, watch},
    task::JoinHandle,
};
use utils::{
    chat::{
        ChatEvent, ChatHandle, Connected, INVITE_TIMEOUT, MAX_MESSAGE_BYTES, accept_invite,
        decline_invite, invite_lan, invite_relay, set_in_chat,
    },
    relay::RelayStatus,
    *,
};

use crate::input::TextInput;

const ACCENT: Color = Color::Rgb(138, 92, 255);
const LAVENDER: Color = Color::Rgb(185, 166, 255);
const MUTED: Color = Color::Rgb(130, 130, 130);
const BORDER: Color = Color::Rgb(70, 70, 70);
const OK: Color = Color::Rgb(60, 207, 145);
const WARN: Color = Color::Rgb(240, 190, 80);
const ERR: Color = Color::Rgb(229, 72, 77);

const TICK: Duration = Duration::from_millis(50);
const SPINNER: [&str; 8] = ["⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷"];

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
    Notice(String),
}

enum Screen {
    Main,
    Pick { selected: usize },
    Waiting(Waiting),
    Chat(ChatView),
}

struct Waiting {
    peer: String,
    joining: bool,
    since: Instant,
    task: JoinHandle<()>,
}

enum UiEvent {
    Connected(Connected),
    Failed(String),
}

#[derive(Clone)]
enum Target {
    Lan(SocketAddr, String),
    Relay(RelayAddr, String),
}

impl Target {
    fn name(&self) -> &str {
        match self {
            Target::Lan(_, name) | Target::Relay(_, name) => name,
        }
    }
}

fn targets(app: &App) -> Vec<Target> {
    let mut list: Vec<Target> = app
        .online
        .snapshot()
        .into_iter()
        .map(|(addr, peer)| Target::Lan(addr, peer.username))
        .collect();
    if let Some(relay_addr) = app.config.relay() {
        list.extend(
            app.relay_state
                .users()
                .into_iter()
                .map(|name| Target::Relay(relay_addr.clone(), name)),
        );
    }
    list
}

enum ChatLine {
    Me(String),
    Peer(String),
    System(String),
}

struct ChatView {
    peer: String,
    fingerprint: String,
    chat: ChatHandle,
    lines: Vec<ChatLine>,
    input: TextInput,
    scroll_up: u16,
    ended: bool,
}

impl ChatView {
    fn new(connected: Connected) -> Self {
        let Connected {
            peer,
            fingerprint,
            chat,
        } = connected;
        Self {
            lines: Vec::new(),
            peer,
            fingerprint,
            chat,
            input: TextInput::default(),
            scroll_up: 0,
            ended: false,
        }
    }

    fn pump(&mut self) {
        while let Some(event) = self.chat.try_event() {
            match event {
                ChatEvent::Message(text) => self.lines.push(ChatLine::Peer(text)),
                ChatEvent::Closed(reason) => {
                    self.ended = true;
                    self.lines.push(ChatLine::System(match reason {
                        None => format!("{} left the chat.", self.peer),
                        Some(e) => format!("Connection lost: {e}"),
                    }));
                }
            }
        }
    }

    fn send(&mut self) {
        let text = self.input.value().trim().to_string();
        if text.is_empty() || self.ended {
            return;
        }
        if text.len() > MAX_MESSAGE_BYTES - 8 {
            self.lines
                .push(ChatLine::System("Message is too long.".into()));
            return;
        }
        if self.chat.send(&text) {
            self.lines.push(ChatLine::Me(text));
            self.input = TextInput::default();
            self.scroll_up = 0;
        }
    }
}

struct NameForm {
    input: TextInput,
    hint: &'static str,
    error: Option<&'static str>,
}

impl NameForm {
    fn new(config: &Config) -> Self {
        Self::with_hint(config, "Others see this name on the LAN and on the relay.")
    }

    fn with_hint(config: &Config, hint: &'static str) -> Self {
        let current = if config.has_name() {
            config.username()
        } else {
            ""
        };
        Self {
            input: TextInput::new(current),
            hint,
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
        Popup::Notice(_) => return key.code == KeyCode::Enter,
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

fn start_task<F>(ui_tx: &mpsc::UnboundedSender<UiEvent>, work: F) -> JoinHandle<()>
where
    F: Future<Output = Result<Connected, String>> + Send + 'static,
{
    let ui_tx = ui_tx.clone();
    tokio::spawn(async move {
        let event = match work.await {
            Ok(connected) => UiEvent::Connected(connected),
            Err(e) => UiEvent::Failed(e),
        };
        let _ = ui_tx.send(event);
    })
}

fn back_to_main(screen: &mut Screen) {
    set_in_chat(false);
    *screen = Screen::Main;
}

async fn event_loop(terminal: &mut DefaultTerminal, app: &mut App) -> io::Result<()> {
    let (ui_tx, mut ui_rx) = mpsc::unbounded_channel::<UiEvent>();
    let mut popup: Option<Popup> = None;
    let mut screen = Screen::Main;

    loop {
        while let Ok(event) = ui_rx.try_recv() {
            match event {
                UiEvent::Connected(connected) if matches!(screen, Screen::Waiting(_)) => {
                    screen = Screen::Chat(ChatView::new(connected));
                }
                UiEvent::Connected(_) => {}
                UiEvent::Failed(msg) => {
                    if matches!(screen, Screen::Waiting(_)) {
                        back_to_main(&mut screen);
                        popup = Some(Popup::Notice(msg));
                    }
                }
            }
        }
        if let Screen::Chat(view) = &mut screen {
            view.pump();
        }

        terminal.draw(|frame| draw(frame, app, &screen, popup.as_ref()))?;

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

        match &mut screen {
            Screen::Main => match key.code {
                KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                KeyCode::Char('r') => app.scan_now.notify_one(),
                KeyCode::Char('1') => popup = Some(Popup::Name(NameForm::new(&app.config))),
                KeyCode::Char('2') => popup = Some(Popup::Network(NetworkForm::new(&app.config))),
                KeyCode::Char('3') if !app.config.has_name() => {
                    popup = Some(Popup::Name(NameForm::with_hint(
                        &app.config,
                        "Set a name first. Until then you stay hidden.",
                    )));
                }
                KeyCode::Char('3') => screen = Screen::Pick { selected: 0 },
                KeyCode::Char('a') => {
                    if let Some(invite) = app.invites.take() {
                        set_in_chat(true);
                        let peer = invite.from.clone();
                        let me = app.config.username().to_string();
                        let task =
                            start_task(&ui_tx, async move { accept_invite(invite, &me).await });
                        screen = Screen::Waiting(Waiting {
                            peer,
                            joining: true,
                            since: Instant::now(),
                            task,
                        });
                    }
                }
                KeyCode::Char('d') => {
                    if let Some(invite) = app.invites.take() {
                        let me = app.config.username().to_string();
                        tokio::spawn(async move { decline_invite(invite, &me).await });
                    }
                }
                _ => {}
            },
            Screen::Pick { selected } => {
                let list = targets(app);
                match key.code {
                    KeyCode::Esc | KeyCode::Char('q') => screen = Screen::Main,
                    KeyCode::Up | KeyCode::Char('k') => *selected = selected.saturating_sub(1),
                    KeyCode::Down | KeyCode::Char('j') => {
                        *selected = (*selected + 1).min(list.len().saturating_sub(1));
                    }
                    KeyCode::Enter => {
                        if let Some(target) = list.get(*selected).cloned() {
                            set_in_chat(true);
                            let me = app.config.username().to_string();
                            let peer = target.name().to_string();
                            let session = app.relay_state.session();
                            let task = start_task(&ui_tx, async move {
                                match target {
                                    Target::Lan(addr, name) => invite_lan(addr, &me, &name).await,
                                    Target::Relay(relay_addr, name) => {
                                        invite_relay(&relay_addr, &me, &name, session).await
                                    }
                                }
                            });
                            screen = Screen::Waiting(Waiting {
                                peer,
                                joining: false,
                                since: Instant::now(),
                                task,
                            });
                        }
                    }
                    _ => {}
                }
            }
            Screen::Waiting(waiting) => {
                if key.code == KeyCode::Esc {
                    waiting.task.abort();
                    back_to_main(&mut screen);
                }
            }
            Screen::Chat(view) => match key.code {
                KeyCode::Esc => back_to_main(&mut screen),
                KeyCode::Enter => view.send(),
                KeyCode::PageUp => view.scroll_up = view.scroll_up.saturating_add(5),
                KeyCode::PageDown => view.scroll_up = view.scroll_up.saturating_sub(5),
                _ if !view.ended => {
                    view.input.handle_key(key);
                }
                _ => {}
            },
        }
    }
}

fn draw(frame: &mut Frame, app: &App, screen: &Screen, popup: Option<&Popup>) {
    if let Screen::Chat(view) = screen {
        draw_chat(frame, view);
        return;
    }
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
    match screen {
        Screen::Pick { selected } => draw_pick(frame, app, *selected),
        Screen::Waiting(waiting) => draw_waiting(frame, waiting),
        _ => {}
    }
    match popup {
        Some(Popup::Name(form)) => draw_name_popup(frame, form),
        Some(Popup::Network(form)) => draw_network_popup(frame, form, app),
        Some(Popup::Notice(msg)) => draw_notice(frame, msg),
        None => {}
    }
    draw_help(frame, help, help_keys(app, screen, popup));
}

fn help_keys(
    app: &App,
    screen: &Screen,
    popup: Option<&Popup>,
) -> Vec<(&'static str, &'static str)> {
    match (popup, screen) {
        (Some(Popup::Name(_)), _) => vec![("Enter", "save"), ("Esc", "cancel")],
        (Some(Popup::Network(_)), _) => vec![
            ("↑↓", "field"),
            ("←→", "interface"),
            ("Enter", "save"),
            ("Esc", "cancel"),
        ],
        (Some(Popup::Notice(_)), _) => vec![("Enter", "ok")],
        (None, Screen::Pick { .. }) => vec![("↑↓", "select"), ("Enter", "invite"), ("Esc", "back")],
        (None, Screen::Waiting(_)) => vec![("Esc", "cancel")],
        (None, Screen::Chat(_)) => {
            vec![("Enter", "send"), ("PgUp/PgDn", "scroll"), ("Esc", "leave")]
        }
        (None, Screen::Main) => {
            let mut keys = vec![
                ("1", "name"),
                ("2", "network"),
                ("3", "connect"),
                ("r", "rescan"),
            ];
            if app.invites.peek().is_some() {
                keys.extend([("a", "accept"), ("d", "decline")]);
            }
            keys.push(("q", "quit"));
            keys
        }
    }
}

fn draw_notice(frame: &mut Frame, msg: &str) {
    let area = centered(frame.area(), 60, 7);
    let inner = popup_frame(frame, area, "Connection");
    frame.render_widget(
        Paragraph::new(msg.to_string())
            .wrap(Wrap { trim: true })
            .alignment(Alignment::Center),
        inner,
    );
}

fn draw_pick(frame: &mut Frame, app: &App, selected: usize) {
    let list = targets(app);
    let height = (list.len() as u16 + 6).clamp(8, 20);
    let area = centered(frame.area(), 60, height);
    let inner = popup_frame(frame, area, "Who do you want to chat with?");
    if list.is_empty() {
        frame.render_widget(
            Paragraph::new(vec![
                Line::default(),
                Line::from("Nobody online yet.").fg(MUTED),
            ])
            .alignment(Alignment::Center),
            inner,
        );
        return;
    }
    let selected = selected.min(list.len() - 1);
    let rows = list.iter().enumerate().map(|(i, target)| {
        let (via, color) = match target {
            Target::Lan(addr, _) => (addr.to_string(), MUTED),
            Target::Relay(..) => ("relay".to_string(), LAVENDER),
        };
        let row = Row::new(vec![
            Cell::from(if i == selected { "▶ " } else { "  " }).fg(ACCENT),
            Cell::from(target.name().to_string()).bold(),
            Cell::from(via).fg(color),
        ]);
        if i == selected {
            row.style(Style::new().bg(Color::Rgb(45, 35, 80)))
        } else {
            row
        }
    });
    let table = Table::new(
        rows,
        [
            Constraint::Length(2),
            Constraint::Fill(1),
            Constraint::Length(22),
        ],
    );
    frame.render_widget(table, inner);
}

fn draw_waiting(frame: &mut Frame, waiting: &Waiting) {
    let area = centered(frame.area(), 56, 7);
    let title = if waiting.joining {
        "Joining"
    } else {
        "Inviting"
    };
    let inner = popup_frame(frame, area, title);
    let elapsed = waiting.since.elapsed();
    let spin = SPINNER[(elapsed.as_millis() / 100) as usize % SPINNER.len()];
    let detail = if waiting.joining {
        "setting up the encrypted connection".to_string()
    } else {
        let left = INVITE_TIMEOUT.saturating_sub(elapsed).as_secs();
        format!("waiting for them to accept · {left}s left")
    };
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(vec![
                Span::from(format!("{spin} ")).fg(ACCENT),
                Span::from(waiting.peer.clone()).bold(),
            ]),
            Line::from(detail).fg(MUTED),
        ])
        .alignment(Alignment::Center),
        inner,
    );
}

fn draw_chat(frame: &mut Frame, view: &ChatView) {
    let [header, body, input, help] = Layout::vertical([
        Constraint::Length(3),
        Constraint::Min(3),
        Constraint::Length(3),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    let head = Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(ACCENT))
        .title(
            Line::from(format!(" Chat with {} ", view.peer))
                .fg(LAVENDER)
                .bold(),
        );
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::from(" TLS 1.3 ").fg(Color::Black).bg(OK).bold(),
            Span::from("  fingerprint ").fg(MUTED),
            Span::from(view.fingerprint.clone()).bold(),
            Span::from("").fg(MUTED),
        ]))
        .block(head),
        header,
    );

    let lines: Vec<Line> = view
        .lines
        .iter()
        .map(|line| match line {
            ChatLine::Me(text) => Line::from(vec![
                Span::from("you").fg(LAVENDER).bold(),
                Span::from(": ").fg(MUTED),
                Span::from(text.clone()),
            ]),
            ChatLine::Peer(text) => Line::from(vec![
                Span::from(view.peer.clone()).fg(OK).bold(),
                Span::from(": ").fg(MUTED),
                Span::from(text.clone()),
            ]),
            ChatLine::System(text) => Line::from(text.clone()).fg(MUTED).italic(),
        })
        .collect();
    let block = panel("Messages".to_string());
    let inner = block.inner(body);
    let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false });
    let total = paragraph.line_count(inner.width) as u16;
    let max_scroll = total.saturating_sub(inner.height);
    let offset = max_scroll.saturating_sub(view.scroll_up.min(max_scroll));
    frame.render_widget(paragraph.scroll((offset, 0)).block(block), body);

    if view.ended {
        frame.render_widget(
            Paragraph::new(Line::from("chat ended · press Esc to go back").fg(MUTED))
                .block(field("message", false)),
            input,
        );
    } else {
        view.input
            .render(frame, input, field("message", true), true);
    }
    draw_help(
        frame,
        help,
        vec![("Enter", "send"), ("PgUp/PgDn", "scroll"), ("Esc", "leave")],
    );
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
    frame.render_widget(Paragraph::new(form.hint).fg(MUTED), hint);
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
            if cfg.has_name() {
                Span::from(cfg.username().to_string()).bold()
            } else {
                Span::from("not set · hidden").fg(WARN).bold()
            },
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
        let (what, key, action) = if app.config.has_name() {
            ("nobody found yet", "r", " to scan")
        } else {
            ("you are hidden until you set a name", "1", " to set it")
        };
        let text = Paragraph::new(vec![
            Line::default(),
            Line::from(what).fg(MUTED),
            Line::from(vec![
                Span::from("press ").fg(MUTED),
                Span::from(key).fg(ACCENT).bold(),
                Span::from(action).fg(MUTED),
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
        RelayStatus::NeedsName => ("○", WARN),
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

fn draw_help(frame: &mut Frame, area: Rect, keys: Vec<(&'static str, &'static str)>) {
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
