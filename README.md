# Marketing Data Validation & Analytics Platform

Portfolio project simulating an end-to-end marketing analytics workflow:
- ETL (Extract, Transform, Load)
- Data Validation (Python)
- SQL/dbt Transformations
- Dashboarding (Power BI / Tableau)
# Marketing-Data-Validation-Analytics-Platform
Portfolio project simulating a marketing data validation &amp; analytics pipeline (ETL, Python, SQL, Power BI/Tableau)

## Quickstart
1. Create and activate a virtual environment:
   ```bash
   python -m venv .venv && source .venv/bin/activate
   ```
2. Install the required Python packages:
   ```bash
   pip install pandas sqlalchemy
   ```
3. Run the ETL pipeline to generate the SQLite database and cleaned CSV:
   ```bash
   python scripts/etl_pipeline.py
   ```
4. Execute the data validation checks:
   ```bash
   python scripts/data_validation.py
   ```
5. Open the generated SQLite database for exploration:
   - Launch [DB Browser for SQLite](https://sqlitebrowser.org/).
   - Choose **Open Database** and select `data/marketing.db` from the project directory.
6. Visualize the cleaned marketing dataset:
   - In **Power BI**, use **Get Data &gt; Text/CSV** and select `data/sample_marketing_data_clean.csv`.
   - In **Tableau**, use **Connect &gt; To a File &gt; Text File** and select the same CSV.

