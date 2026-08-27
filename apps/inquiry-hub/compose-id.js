/**
 * このアプリ発の新規メールに入れる仮の外部ID (2026-08-27)
 *
 * Gmail のスレッドIDは送ってみるまで分からないため、送信前は 'compose:<uuid>' を入れておき、
 * 送信成功時に実スレッドIDへ差し替える (compose.js / outbox.js)。
 *
 * 判定だけを独立したモジュールに置いているのは循環importを避けるため
 * (compose.js は Gmailアダプターの定数を参照し、Gmailアダプターはこの判定を参照する)。
 */
export const COMPOSE_PREFIX = 'compose:';

/** 外部スレッドがまだ確定していない (=このアプリ発の未送信メール) か */
export const isComposeThread = extId => String(extId || '').startsWith(COMPOSE_PREFIX);
