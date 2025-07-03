# Crypto Data Fetcher

Bybitの公開データから仮想通貨のティックデータをダウンロードし、1秒足OHLCVデータに変換するシンプルなツールです。

## 機能

- 📥 **自動ダウンロード**: Bybitからティックデータ（.csv.gz）を自動ダウンロード
- 🔄 **データ変換**: ティックデータを1秒足OHLCV形式に変換
- ✅ **重複スキップ**: 既にダウンロード済みのファイルは自動的にスキップ
- 🔁 **自動リトライ**: エラー時の自動リトライ（最大3回）
- 📊 **進捗表示**: 処理状況をリアルタイムで表示

## インストール

```bash
# リポジトリのクローン
git clone <repository_url>
cd crypto-data-fetcher

# 仮想環境の作成（Python 3.8以上推奨）
python3 -m venv venv

# 仮想環境の有効化
# macOS/Linux:
source venv/bin/activate
# Windows:
# venv\Scripts\activate

# 依存関係のインストール
pip install -r requirements.txt
```

## 使い方

仮想環境が有効化されていることを確認してから実行してください。

```bash
# 仮想環境の有効化（まだの場合）
source venv/bin/activate  # macOS/Linux
# venv\Scripts\activate  # Windows

# 基本的な使い方
python download.py BTCUSDT 2024-01-01 2024-01-31

# 出力ディレクトリを指定
python download.py BTCUSDT 2024-01-01 2024-01-31 --output-dir /path/to/data

# リトライ回数を変更
python download.py BTCUSDT 2024-01-01 2024-01-31 --max-retries 5

# 使用後は仮想環境を無効化
deactivate
```

### パラメータ

- `symbol`: 取引ペア（例：BTCUSDT, ETHUSDT, BTCPERP, ETHUSD）
- `start_date`: 開始日（YYYY-MM-DD形式）
- `end_date`: 終了日（YYYY-MM-DD形式）
- `--output-dir`: 出力ディレクトリ（デフォルト：data）
- `--max-retries`: ダウンロード失敗時の最大リトライ回数（デフォルト：3）

## 出力形式

データは以下のディレクトリ構造で保存されます：

```
data/
└── BTCUSDT/
    ├── BTCUSDT_2024-01-01_1sec.csv
    ├── BTCUSDT_2024-01-02_1sec.csv
    └── ...
```

### OHLCVデータ形式

```csv
timestamp,datetime,open,high,low,close,volume,trades
1704067200,2024-01-01 00:00:00,42000.0,42100.0,41950.0,42050.0,150.5,25
1704067201,2024-01-01 00:00:01,42050.0,42075.0,42025.0,42060.0,75.3,12
```

## 注意事項

- Bybitのデータは2020年3月25日から利用可能です
- 未来の日付は指定できません
- 大量のデータをダウンロードする場合は、Bybitのサーバーに負荷をかけないよう適度な間隔を空けてください

## エラーが発生した場合

特定の日付でダウンロードに失敗した場合、同じコマンドを再実行することで、失敗した日付のみを再ダウンロードできます（既に成功したファイルはスキップされます）。

```bash
# 例：2024-01-15のみ失敗した場合
python download.py BTCUSDT 2024-01-01 2024-01-31
# → 2024-01-15のみ再ダウンロードされる
```

## その他のツール

### ティックデータ変換スクリプト（単体使用）

既にダウンロード済みのティックデータを変換する場合：

```bash
python scripts/convert_to_1sec_ohlcv.py input.csv -o output_1sec.csv
```

## ライセンス

MIT License