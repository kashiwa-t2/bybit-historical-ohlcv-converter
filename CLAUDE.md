# CLAUDE.md - シンプル版プロジェクトガイド

このファイルは、Claude Code (claude.ai/code) がこのリポジトリで作業する際のガイドラインです。

## 🎯 プロジェクト概要

**Crypto Data Fetcher** - Bybitの公開データから仮想通貨のティックデータをダウンロードし、複数の時間足（1秒、1分、5分、15分、1時間、4時間）のOHLCVに変換するシンプルなツール。全時間足を一度に出力することも可能

### 主な目標
1. 指定した銘柄と期間のティックデータをダウンロード
2. 複数の時間足でOHLCV形式に変換（1s, 1m, 5m, 15m, 1h, 4h）
3. 銘柄ごとにフォルダ分けして保存
4. 重複ダウンロードの防止
5. エラー時の自動リトライ

## 📁 プロジェクト構造

```
crypto-data-fetcher/
├── download.py                  # メインスクリプト
├── scripts/
│   └── convert_to_ohlcv.py      # 複数時間足対応の変換スクリプト
├── data/                        # 出力ディレクトリ
│   └── BTCUSDT/                # 銘柄ごとのフォルダ
│       ├── BTCUSDT_2024-01-01_1s.csv
│       ├── BTCUSDT_2024-01-01_5m.csv
│       └── temp/               # 一時ファイル用
├── venv/                        # 仮想環境（.gitignoreで除外）
├── requirements.txt             # 最小限の依存関係
├── README.md                   # プロジェクト説明
└── CLAUDE.md                   # このファイル
```

## 🚀 使い方

### 環境セットアップ
```bash
# 仮想環境の作成
python3 -m venv venv

# 仮想環境の有効化
source venv/bin/activate  # macOS/Linux
# venv\Scripts\activate  # Windows

# 依存関係のインストール
pip install -r requirements.txt
```

### 基本的な使用例
```bash
# 仮想環境が有効化されている状態で実行
# 1ヶ月分のデータをダウンロード（デフォルトは1分足）
python download.py BTCUSDT 2024-01-01 2024-01-31

# 全ての時間足を一度に作成（1s, 1m, 5m, 15m, 1h, 4h）
python download.py BTCUSDT 2024-01-01 2024-01-31 -t all

# 特定の時間足のみ作成
python download.py BTCUSDT 2024-01-01 2024-01-31 -t 5m
python download.py BTCUSDT 2024-01-01 2024-01-31 --timeframe 1h

# 特定の日付で全時間足を作成
python download.py BTCUSDT 2024-01-15 2024-01-15 -t all

# カスタム出力ディレクトリ
python download.py BTCUSDT 2024-01-01 2024-01-31 -t all --output-dir /path/to/data

# 使用後は仮想環境を無効化
deactivate
```

## 💡 実装の特徴

### シンプルな設計
- 単一のスクリプト（download.py）で完結
- 標準ライブラリを最大限活用
- 外部依存は最小限（tqdm、python-dateutil）
- SQLiteなどのDBは使用しない

### エラーハンドリング
```python
# 自動リトライ（指数バックオフ）
for attempt in range(max_retries):
    try:
        download_file()
        break
    except:
        time.sleep(2 ** attempt)  # 1秒、2秒、4秒...
```

### 重複チェック
```python
# ファイルの存在確認のみでシンプルに
if output_file.exists():
    print("Already exists, skipping")
    return True
```

## 📝 データ仕様

### Bybitダウンロードページ
- URL: https://public.bybit.com/trading/BTCUSDT/
- ファイル形式: `BTCUSDT[YYYY-MM-DD].csv.gz`
- データ範囲: 2020-03-25から現在まで

### 出力フォーマット
```
data/
└── BTCUSDT/
    ├── BTCUSDT_2024-01-01_1sec.csv  # 1秒足OHLCV
    ├── BTCUSDT_2024-01-02_1sec.csv
    └── temp/                         # 一時ファイル（自動削除）
```

### OHLCVファイル形式
```csv
timestamp,datetime,open,high,low,close,volume,trades
1704067200,2024-01-01 00:00:00,42000.0,42100.0,41950.0,42050.0,150.5,25
```

## 🔧 メンテナンス・拡張時の注意

### 新しい銘柄を使用する場合
- Bybitの公開ページ（https://public.bybit.com/trading/）で利用可能な銘柄を確認
- 銘柄名は英数字のみ（例：BTCUSDT, BTCPERP, ETHUSD）
- Bybitのディレクトリ構造に従う（/trading/[SYMBOL]/）

### エラー処理の改善
- 現在は3回リトライで固定
- 必要に応じて--max-retriesパラメータで調整可能

### パフォーマンス
- 1日分のデータ（約500万行）の処理に約1-2分
- 並列ダウンロードは実装していない（サーバー負荷を考慮）

## 🎯 今後の拡張案（必要になった場合）

1. **並列ダウンロード**
   - 複数日を同時にダウンロード
   - asyncioを使用した実装

2. **データ検証**
   - ダウンロード後のファイルサイズチェック
   - OHLCVデータの整合性確認

3. **進捗の永続化**
   - 大量データ処理時の中断・再開機能
   - ただしファイルベースでシンプルに

---

*最終更新: 2025-01-03*
*このシンプルな実装が現在の要件に最適です*