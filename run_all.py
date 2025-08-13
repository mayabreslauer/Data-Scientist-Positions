from linkedin_scraper import crawl_serper, upsert_and_mark_stale, CSV_PATH as LINKEDIN_OUT
from taboola_scraper import scrape_taboola_jobs, upsert_and_mark_stale as upsert_taboola
from similarweb_scraper import scrape_similarweb_jobs, upsert_and_mark_stale as upsert_similarweb
from melio_scraper import scrape_melio_jobs, upsert_and_mark_stale as upsert_melio
from riskified_scraper import scrape_riskified_jobs, upsert_and_mark_stale as upsert_riskified
from merge_jobs import merge_job_csvs

def main():
    print("=== LinkedIn via Serper ===")
    linkedin_rows = crawl_serper()
    upsert_and_mark_stale(LINKEDIN_OUT, linkedin_rows)

    print("=== Taboola ===")
    taboola_rows = scrape_taboola_jobs("Data Scientist")
    upsert_taboola("taboola_ds_jobs.csv", taboola_rows)

    print("=== Similarweb ===")
    sim_rows = scrape_similarweb_jobs("Data Scientist")
    upsert_similarweb("similarweb_ds_jobs.csv", sim_rows)

    print("=== Melio ===")
    melio_rows = scrape_melio_jobs("Data Scientist")
    upsert_melio("melio_ds_jobs.csv", melio_rows)

    print("=== Riskified ===")
    risk_rows = scrape_riskified_jobs("Data Scientist")
    upsert_riskified("riskified_ds_jobs.csv", risk_rows)

    print("=== Merge CSVs ===")
    merge_job_csvs(
        input_globs=[
            LINKEDIN_OUT,
            "riskified_ds_jobs.csv",
            "similarweb_ds_jobs.csv",
            "taboola_ds_jobs.csv",
            "melio_ds_jobs.csv",
        ],
        output_path="merged_jobs.csv"
    )

if __name__ == "__main__":
    main()


