/**
 * seed-data.js — 「選べるセット」設定の初期値
 *
 * 選択肢の解決は RMS Item API 2.0 の customizationOptions を第一の情報源にするが、
 * 🚨 RMS は「楽天の・今の」選択肢しか持たない。
 *    - 他モール (Yahoo!/Qoo10/auPAY) は楽天より品揃えが多いことがある
 *    - 楽天で廃止した香りも、過去の受注や他モールにはまだ現れる
 *    実測 (2026-08-07): RMSだけだと ae5ml-select5 の解決率が 16.7% まで落ちた。
 * そのため seed-mappings.json (既存Excel由来 + 実受注から見つけた別名) を必ずマージする。
 *
 * 運用開始後の追加・修正は管理画面から行い、このファイルは初回シードにのみ使う。
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/** 選べるセットの商品コード。RMSから選択肢を引くときのキーになる */
export const SET_CODES = [
  'selectae10-5',
  'selectwa10-3',
  'selectwa10-5',
  'ae5ml-select5',
  'ganesh-select3',
  'selecteo10ml3',
  'selecteo10ml5',
  'selecteo10ml8',
  'selectam20ml3',
];

/**
 * 手動マッピング (セット商品コード → [{option, code}])。
 * option は「モールの商品OPに現れる文字列」。正規化して突き合わせるので
 * 括弧・全角半角・スペース・アンダースコアの違いは吸収される。
 *
 * 内訳 (2026-08-07 時点で214行):
 *   - ganesh-select3 39行 … 選択肢に商品コードが入っていない (文字数制限) ので全件必要
 *   - selectwa10-3/-5, selectae10-5, ae5ml-select5 … 表示名だけで送ってくるモール用
 *   - selecteo10ml3/5/8 … 楽天の選択肢が文字数制限で切れている
 *     (hakka-eucaly → hakka-eucalyptus / hakka-lemon → hakka-lemongrass)
 *   - selectae10-5 の「キンモクセイ(金木犀)」「サンダルウッド(白檀)」
 *     「ホワイトリリー（しらゆり）」… モール側の表示名が商品マスタと違う
 */
export const MANUAL_MAPPINGS = JSON.parse(
  fs.readFileSync(path.join(__dirname, 'seed-mappings.json'), 'utf8')
);

/**
 * おまけ (シークレットプレゼント) の優先順位。全セット共通。
 * 上から見て「利用可能在庫がある最初の1つ」を採用する。
 * ⭐並びは原価の安い順 (中原さん指示 2026-08-07)。原価16円グループがちょうど10件あり、
 *   次点は nemunemask の25円、その次は sbs-* / kusobathroom-* の95円と6倍に跳ねるので
 *   16円グループで打ち止めにしている。同一原価の中は在庫の多い順。
 */
export const OMAKE_PRIORITY = [
  'nemune-s',       // ねむね すやすや (シトラス)   原価16
  'nemune-g',       // ねむね ぐっすり (ラベンダー) 原価16
  'petirfleur-wr',  // ローズガーデンバス ホワイトローズ
  'petirfleur-br',  // ローズガーデンバス ブラックローズ
  'richbathp-sc',   // リッチバスパウダー スパークリングカクテル
  'richbathp-gf',   // リッチバスパウダー グレープフルーツ
  'richbathp-yz',   // リッチバスパウダー 柚子
  'richbathp-fc',   // リッチバスパウダー フルーツカクテル
  'richbathp-sl',   // リッチバスパウダー 瀬戸内レモン
  'richbathp-cf',   // リッチバスパウダー シトラスフルーツ
];
