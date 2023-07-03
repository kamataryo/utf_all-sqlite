import fs from 'node:fs/promises'
import { createReadStream } from 'node:fs'
import { parse } from 'csv-parse'
import Database from 'better-sqlite3'
import os from 'node:os'

const BASE_PATH = `${os.homedir()}/.utf_all-sqlite`;
const LAST_MODIFIED = `${BASE_PATH}/last-modified`;
const DATA_PATH = `${BASE_PATH}/data.csv`;
const SQLITE_PATH = `${BASE_PATH}/data.sqlite`;
const ENDPOINT = "https://www.post.japanpost.jp/zipcode/utf_all.csv"

const headers: { name: string, desc: string, type: 'TEXT' | 'INTEGER', index?: boolean }[] = [
  { name: 'jiscode', desc: "全国地方公共団体コード", type: "TEXT" }, // （JIS X0401、X0402）………　半角数字
  { name: 'zip5', desc: "（旧）郵便番号", type: "TEXT" }, //（5桁）………………………………………　半角数字
  { name: 'zip7', desc: "郵便番号（7桁）", type: "TEXT" }, //………………………………………　半角数字
  { name: 'pref_kana', desc: "都道府県名カナ", type: "TEXT" }, //　…………　半角カタカナ（コード順に掲載）　（注1）
  { name: 'city_kana', desc: "市区町村名カナ", type: "TEXT" }, //　…………　半角カタカナ（コード順に掲載）　（注1）
  { name: 'town_kana', desc: "町域名カナ", type: "TEXT" }, //　………………　半角カタカナ（五十音順に掲載）　（注1）
  { name: 'pref', desc: "都道府県名", type: "TEXT", index: true }, //　…………　漢字（コード順に掲載）　（注1,2）
  { name: 'city', desc: "市区町村名", type: "TEXT", index: true }, //　…………　漢字（コード順に掲載）　（注1,2）
  { name: 'town', desc: "町域名", type: "TEXT" }, //　………………　漢字（五十音順に掲載）　（注1,2）
  { name: 'multi_zip_in_single_town', desc: "一町域が二以上の郵便番号で表される場合の表示", type: "INTEGER" }, //　（注3）　（「1」は該当、「0」は該当せず）
  { name: 'koaza_banchi', desc: "小字毎に番地が起番されている町域の表示", type: "INTEGER" }, //　（注4）　（「1」は該当、「0」は該当せず）
  { name: 'has_chome', desc: "丁目を有する町域の場合の表示", type: "INTEGER" }, //　（「1」は該当、「0」は該当せず）
  { name: 'single_zip_for_multi_town', desc: "一つの郵便番号で二以上の町域を表す場合の表示", type: "INTEGER" }, //　（注5）　（「1」は該当、「0」は該当せず）
  { name: 'updated', desc: "更新の表示", type: "INTEGER" }, //（注6）（「0」は変更なし、「1」は変更あり、「2」廃止（廃止データのみ使用））
  { name: 'updated_reason', desc: "変更理由", type: "INTEGER" }, //　（「0」は変更なし、「1」市政・区政・町政・分区・政令指定都市施行、「2」住居表示の実施、「3」区画整理、「4」郵便区調整等、「5」訂正、「6」廃止（廃止データのみ使用））
]

const main = async () => {
  await fs.mkdir(BASE_PATH, { recursive: true });

  const [previouslyLastModified, currentLastModified, dataExists] = await Promise.all([
    fs.readFile(LAST_MODIFIED, 'utf-8').catch(() => null),
    fetch(ENDPOINT, { method: 'HEAD' }).then(resp => resp.headers.get('Last-Modified')).catch(() => null),
    fs.open(DATA_PATH).then(() => true).catch(() => false),
  ]);

  if(dataExists && previouslyLastModified === currentLastModified) {
    console.log(`[INFO] 最終更新日時: ${currentLastModified} は前回と同じため、ダウンロードをスキップします`);
  } else {
    console.log(`[INFO] utf_all.csv のダウンロード中`);
    const resp = await fetch(ENDPOINT)
    if(resp.body) {
      const file = await fs.open(DATA_PATH, 'w');
      // @ts-ignore
      for await (const chunk of resp.body) {
        await file.write(chunk);
      }
      await file.close()
      await fs.writeFile(LAST_MODIFIED, resp.headers.get('Last-Modified') || '')
    } else {
      throw new Error("Failed to get readable stream from response body");
    }
  }

  console.log(`[INFO] utf_all.csv から SQLite への変換中`);
  const db = new Database(SQLITE_PATH)
  db.exec(`DROP TABLE IF EXISTS utf_all;`)
  db.exec(`CREATE TABLE utf_all (
    ${headers.map(({ name, type }) => `${name} ${type}`).join(',\n    ')}
  );`)
  const index_targets = headers.filter(({ index }) => index).map(({ name }) => name)
  for (const index_target of index_targets) {
    db.exec(`CREATE INDEX ide_${index_target} ON utf_all (${index_target});`)
  }

  const stms = db.prepare(`INSERT INTO utf_all
    (${headers.map(({ name }) => name).join(', ')})
    VALUES (${headers.map(({ name }) => `$${name}`).join(', ')});
  `);

  const transaction = db.transaction((rows) => {
      for (const row of rows) {
        stms.run(row);
      }
  });

  const csvParseStream = createReadStream(DATA_PATH)
    .pipe(parse({ delimiter: ',' }))

  const current_rows = []
  for await (const row of csvParseStream) {
    const item = (row as string[]).reduce<{ [key: string]: string }>((prev, value, index) => {
      prev[headers[index].name] = value
      return prev
    }, {})
    current_rows.push(item)
    if(current_rows.length % 10000 === 0) {
      transaction(current_rows)
      current_rows.length = 0
    }
  }
  transaction(current_rows)

}


main()
