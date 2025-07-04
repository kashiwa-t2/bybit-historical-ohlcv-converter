# Bybit Historical OHLCV Converter

Bybit の公開データから仮想通貨のティックデータをダウンロードし、複数の時間足で OHLCV データに変換するツールです。

## 🚀 クイックスタート

### 1. セットアップ

```bash
# リポジトリのクローン
git clone <repository_url>
cd bybit-historical-ohlcv-converter

# 仮想環境の作成と有効化
python3 -m venv venv
source venv/bin/activate  # macOS/Linux
# venv\Scripts\activate    # Windows

# 依存関係のインストール
pip install -r requirements.txt
```

### 2. 使い方（対話形式）

```bash
python download.py
```

すると、このような対話が始まります：

```
==================================================
  Bybit Historical Data Downloader
==================================================

利用可能な取引ペア: https://public.bybit.com/trading/
※ USDTは自動的に追加されます（例: BTC → BTCUSDT）
取引ペアを入力してください (例: BTC): BTC

市場タイプを選択してください:
  [1] 先物 (Futures)
  [2] 現物 (Spot)
  [3] 両方 (Both)
選択 [1]: 1

ダウンロードする期間を選択してください:
  [1] 利用可能な全ての期間
  [2] 期間を指定する
  [3] 指定した日付から最新まで
  [4] 最古から指定した日付まで
選択 [1]: 1

時間足を選択してください:
  [1] 全ての時間足 (1s, 1m, 5m, 15m, 1h, 4h, 1d)
  [2] カスタム選択
選択 [1]: 1

出力ディレクトリ [data]: 

==============================
  設定確認
==============================
取引ペア: BTCUSDT
期間: 全期間 (2020-03-25 ～ 2025-01-03)
時間足: 全ての時間足 (1s, 1m, 5m, 15m, 1h, 4h, 1d)
出力先: data/

この設定で実行しますか？ [Y/n]: y
```

設定完了後、自動的にダウンロードと変換が実行されます。

### 3. 出力結果

データは以下のような構造で保存されます：

```
data/
└── BTCUSDT/
    ├── futures/
    │   ├── BTCUSDT_2020-03-25_1s.csv
    │   ├── BTCUSDT_2020-03-25_1m.csv
    │   ├── BTCUSDT_2020-03-25_5m.csv
    │   ├── BTCUSDT_2020-03-25_1h.csv
    │   ├── BTCUSDT_2020-03-25_4h.csv
    │   ├── BTCUSDT_2020-03-25_1d.csv
    │   └── ...
    └── spot/
        ├── BTCUSDT_2022-11-10_1s.csv
        ├── BTCUSDT_2022-11-10_1m.csv
        └── ...
```

各CSVファイルの形式：
```csv
timestamp,datetime,open,high,low,close,volume,trades
1585094400,2020-03-25 00:00:00,6543.5,6545.0,6540.0,6544.0,123.45,25
```

## 📋 対話形式の詳細

### 期間選択の説明

1. **利用可能な全ての期間**: その取引ペアで利用可能な全データを自動取得
2. **期間を指定する**: 開始日と終了日を手動で入力
3. **指定した日付から最新まで**: 指定した日から最新データまで
4. **最古から指定した日付まで**: 最古データから指定した日まで

### 時間足の選択肢

1. **全ての時間足**: 1秒、1分、5分、15分、1時間、4時間、1日足を一度に作成
2. **カスタム選択**: 必要な時間足だけを選択（複数選択可能）

カスタム選択の例：
```
選択: 2,5,7        # 1分、1時間、1日足のみ
選択: 1 3 5        # 1秒、5分、1時間足のみ
```

## ⚠️ 注意事項

- **USDTペア専用**: このツールはUSDTペア専用です。入力時にUSDTは自動的に追加されます（例: BTC → BTCUSDT）
- **市場タイプ**: 先物（futures）と現物（spot）のデータは別々に保存されます
- **データ期間の違い**: 
  - 先物: 通常2020年頃から利用可能
  - 現物: 通常2022年11月頃から利用可能
- **取引ペアの確認**: 使用前に https://public.bybit.com/trading/ （先物）または https://public.bybit.com/spot/ （現物）で対象の取引ペアが利用可能であることを確認してください
- 未来の日付は指定できません

## 🔧 エラーが発生した場合

特定の日付でダウンロードに失敗した場合、同じコマンドを再実行することで、失敗した日付のみを再ダウンロードできます（既に成功したファイルはスキップされます）。

## 🔨 CLI形式

オプションを直接指定して実行することもできます：

```bash
# 全期間のデータをダウンロード（先物・デフォルト）
# USDTは自動的に追加されます
python download.py BTC --full -t all

# 現物市場のデータをダウンロード
python download.py BTC --start 2024-01-01 --end 2024-01-31 --market-type spot

# 先物と現物の両方をダウンロード
python download.py ETH --full --market-type both

# 開始日から最新まで（先物）
python download.py BTC --start 2024-01-01

# 最古から終了日まで（現物）
python download.py BTC --end 2024-01-31 --market-type spot

# 特定の時間足のみ
python download.py BTC --full -t 5m

# カスタム出力ディレクトリ
python download.py BTC --full --output-dir /path/to/data
```

### CLI オプション一覧

```bash
python download.py --help
```

でヘルプと使用例を確認できます。

## 🛠️ その他のツール

### 既存ファイルの変換

既にダウンロード済みのティックデータを変換する場合：

```bash
python scripts/convert_to_ohlcv.py input.csv -t 5m -o output_5m.csv
python scripts/convert_to_ohlcv.py input.csv -t 1h -o output_1h.csv
```

### 使用後の片付け

```bash
# 仮想環境を無効化
deactivate
```

## 📝 機能一覧

- ✅ **対話形式**: ガイド付きで設定を選択
- ✅ **USDT自動追加**: シンボル入力時にUSDTを自動的に追加（BTC → BTCUSDT）
- ✅ **先物・現物対応**: 先物（futures）と現物（spot）の両方のデータに対応
- ✅ **自動ダウンロード**: Bybit からティックデータ（.csv.gz）を自動ダウンロード
- ✅ **データ変換**: ティックデータを複数の時間足（1 秒、1 分、5 分、15 分、1 時間、4 時間、1 日）の OHLCV 形式に変換
- ✅ **ギャップフィリング**: 取引がない時間帯も前回終値で補完し、連続したデータを生成
- ✅ **重複スキップ**: 既にダウンロード済みのファイルは自動的にスキップ
- ✅ **自動リトライ**: エラー時の自動リトライ（最大 3 回）
- ✅ **進捗表示**: 処理状況をリアルタイムで表示
- ✅ **日付範囲自動取得**: 取引ペアごとの利用可能期間を自動判定
- ✅ **CLI形式**: コマンドライン操作

## 📄 ライセンス

MIT License
