/**
 * 送料テーブル管理（shipping.json ベース）
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', '..', 'data');
const FILE = path.join(DATA_DIR, 'shipping.json');

// 初期データ（shipping.json が存在しない場合に使用）
const DEFAULT_DATA = [
  {"name":"定形内（25g以内）","total":146,"shipping":110,"work":10,"material":10,"labor":16,"size":"長辺23.5cm以内、短辺12cm以内、厚さ1cm以内","weight":"25","delivery":"ポスト投函"},
  {"name":"定形内（50g以内）","total":146,"shipping":110,"work":10,"material":10,"labor":16,"size":"長辺23.5cm以内、短辺12cm以内、厚さ1cm以内","weight":"50","delivery":"ポスト投函"},
  {"name":"定形外規格内（50g以内）","total":182,"shipping":140,"work":10,"material":16,"labor":16,"size":"長辺34cm以内、短辺25cm以内、厚さ3cm以内","weight":"50","delivery":"ポスト投函"},
  {"name":"定形外規格内（100g以内）","total":222,"shipping":180,"work":10,"material":16,"labor":16,"size":"長辺34cm以内、短辺25cm以内、厚さ3cm以内","weight":"100","delivery":"ポスト投函"},
  {"name":"ネコポス","total":237,"shipping":198,"work":0,"material":23,"labor":16,"size":"長辺23～31.2cm以内、短辺11.5～22.8cm以内、厚さ3cm以内","weight":"1000","delivery":"ポスト投函"},
  {"name":"クリックポスト","total":261,"shipping":185,"work":10,"material":50,"labor":16,"size":"長辺14～34cm以内、短辺9～25cm以内、厚さ3cm以内","weight":"1000","delivery":"ポスト投函"},
  {"name":"楽天倉庫（RSL）ポスト投函サイズ","total":277,"shipping":198,"work":55,"material":24,"labor":0,"size":"長辺：32cm未満、短辺：23cm未満、高さ：2.7cm未満","weight":"","delivery":"手渡し"},
  {"name":"定形外規格外（50g以内）","total":302,"shipping":260,"work":10,"material":16,"labor":16,"size":"長辺60cm以内かつ長辺+短辺+厚さの合計が90cm以下","weight":"50","delivery":"ポスト投函"},
  {"name":"定形外規格内（150g以内）","total":312,"shipping":270,"work":10,"material":16,"labor":16,"size":"長辺34cm以内、短辺25cm以内、厚さ3cm以内","weight":"150","delivery":"ポスト投函"},
  {"name":"定形外規格外（100g以内）","total":332,"shipping":290,"work":10,"material":16,"labor":16,"size":"長辺60cm以内かつ長辺+短辺+厚さの合計が90cm以下","weight":"100","delivery":"ポスト投函"},
  {"name":"定形外規格内（250g以内）","total":362,"shipping":320,"work":10,"material":16,"labor":16,"size":"長辺34cm以内、短辺25cm以内、厚さ3cm以内","weight":"250","delivery":"ポスト投函"},
  {"name":"ゆうパケットパフ","total":424,"shipping":374,"work":10,"material":20,"labor":30,"size":"7cmの箱もしくは専用袋","weight":"1000","delivery":"置き配"},
  {"name":"定形外規格外（150g以内）","total":432,"shipping":390,"work":10,"material":16,"labor":16,"size":"長辺60cm以内かつ長辺+短辺+厚さの合計が90cm以下","weight":"150","delivery":"ポスト投函"},
  {"name":"宅急便50サイズ","total":481,"shipping":429,"work":0,"material":22,"labor":30,"size":"3辺の長さが50cm以内","weight":"2000","delivery":"手渡し"},
  {"name":"定形外規格外（250g以内）","total":492,"shipping":450,"work":10,"material":16,"labor":16,"size":"長辺60cm以内かつ長辺+短辺+厚さの合計が90cm以下","weight":"250","delivery":"ポスト投函"},
  {"name":"楽天倉庫（RSL）60サイズ","total":537,"shipping":418,"work":88,"material":31,"labor":0,"size":"3辺の長さが60cm以内","weight":"","delivery":"手渡し"},
  {"name":"宅急便60サイズ","total":544,"shipping":484,"work":0,"material":30,"labor":30,"size":"3辺の長さが60cm以内","weight":"2000","delivery":"手渡し"},
  {"name":"楽天倉庫（RSL）80サイズ","total":549,"shipping":418,"work":88,"material":43,"labor":0,"size":"3辺の長さが80cm以内","weight":"","delivery":"手渡し"},
  {"name":"楽天倉庫（RSL）100サイズ","total":562,"shipping":418,"work":88,"material":56,"labor":0,"size":"3辺の長さが100cm以内","weight":"","delivery":"手渡し"},
  {"name":"レターパック","total":570,"shipping":520,"work":10,"material":10,"labor":30,"size":"レターパックの封筒に入るもの","weight":"4000","delivery":"手渡し"},
  {"name":"宅急便80サイズ","total":609,"shipping":539,"work":0,"material":40,"labor":30,"size":"3辺の長さが80cm以内","weight":"5000","delivery":"手渡し"},
  {"name":"宅急便100サイズ","total":810,"shipping":660,"work":0,"material":120,"labor":30,"size":"3辺の長さが100cm以内","weight":"10000","delivery":"手渡し"},
  {"name":"宅急便140サイズ","total":945,"shipping":715,"work":0,"material":200,"labor":30,"size":"3辺の長さが140cm以内","weight":"15000","delivery":"手渡し"},
];

export function loadShipping() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  if (!fs.existsSync(FILE)) {
    fs.writeFileSync(FILE, JSON.stringify(DEFAULT_DATA, null, 2), 'utf-8');
    return DEFAULT_DATA;
  }
  return JSON.parse(fs.readFileSync(FILE, 'utf-8'));
}
