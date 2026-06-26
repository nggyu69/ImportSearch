# 📦 ImportSearch

> A Django web app for searching and analyzing Indian import / customs (Bill of Entry) trade data — fast full‑text search across millions of shipment records, with one‑click Excel export and bulk BOM pricing lookups.

> 🚀 **Check out the upgraded ui version of the project:** [**importsearch_next**](https://github.com/nggyu69/importsearch_next) — a Next.js rewrite of this project with a refreshed, modern interface.

---

## ✨ Features

- 🔎 **Full‑text search** — query shipment records by **Supplier**, **Importer**, and **Product Description** over any date range, powered by SQLite **FTS5** with trigram tokenization.
- ⚡ **Parallel queries** — each year is searched in its own process (Python `multiprocessing`), so multi‑year searches stay fast.
- 📤 **Flexible export** — view results in an interactive in‑browser grid, or download the full result set as a formatted **Excel** file (auto‑fit columns, filters, frozen header).
- 📥 **Data ingestion** — upload a `.zip` of monthly Excel files; they're auto‑sorted by year/month, normalized (handles standard *and* non‑standard column formats), and loaded into per‑year tables.
- 📊 **Live progress tracking** — ingestion runs in the background with a real‑time progress bar (`ProcessingTask` model + polling endpoint).
- 🧾 **BOM pricing search** — upload a Bill of Materials spreadsheet and get back a workbook with a hyperlinked pricing sheet for every part number (MPN), sorted by unit price.
- 🗓️ **Coverage view** — see exactly which months of data are loaded and how many rows each contains.

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| 🐍 Backend | Python 3.10+, Django 5.x |
| 🗄️ Database | SQLite + FTS5 (per‑year tables, trigram full‑text index) |
| 🧮 Data | pandas, NumPy, python‑calamine, openpyxl, XlsxWriter |
| 🌐 Frontend | Django templates, HTML/CSS, in‑browser results grid |

---

## 📁 Project Structure

```
ImportSearch/
├── ImportSearch/        # Django project (settings, urls, wsgi/asgi)
├── SearchApp/           # Main app — views, models, migrations
├── Scripts/             # Data ingestion & table-building utilities
│   └── create_table.py  # Excel → SQLite loader + FTS5 setup
├── templates/           # HTML templates (search, results, upload, BOM…)
├── static/              # CSS, images
└── manage.py
```

---

## 🚀 Getting Started

### 1. Install dependencies

Using **Poetry**:

```bash
poetry install
poetry shell
```

…or with **Pipenv**:

```bash
pipenv install
pipenv shell
```

### 2. Set up the database

```bash
python manage.py migrate
```

### 3. Run the server

```bash
python manage.py runserver
```

Then open **http://127.0.0.1:8000/** in your browser (or `http://<your-ip>:8000/` from another device on the same network).

---

## 🧭 Usage

| Page | Route | What it does |
|------|-------|--------------|
| 🔍 Search | `/` | Search by supplier / importer / product + date range; view or export results |
| 📋 Results | `/results/` | Interactive grid of the latest search |
| ⬆️ Insert | `/insert/` | Upload a `.zip` of monthly Excel files for ingestion |
| 🔩 BOM Search | `/search_bom/` | Upload a BOM to generate per‑part pricing |
| ⏳ Loading | `/loading/<task_id>/` | Live ingestion progress |
| 🗓️ Months | `/display-months/` | View loaded data coverage |

---

## 📝 Notes

- Data files are organized on disk under `Data/Excel_Files/<year>/<month>/` and loaded into `Data/Databases/Data.sqlite3`.
- Search results are cached on disk under `Data/Results/` to avoid recomputing identical queries.
- Data coverage starts from **2018** onward.
