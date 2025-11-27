# Kimball vs. Inmon: A Practical Python Lesson

This project provides a hands-on comparison of the two dominant data warehousing methodologies: **Bill Inmon's Corporate Information Factory (CIF)** and **Ralph Kimball's Dimensional Modeling**.

We have built a mini-project that simulates an E-commerce environment to demonstrate these concepts using Python and SQLite.

## Project Structure

-   `generate_data.py`: Generates synthetic E-commerce data (OLTP).
-   `inmon_etl.py`: Implements the Inmon approach (Normalized 3NF EDW).
-   `kimball_etl.py`: Implements the Kimball approach (Star Schema Data Mart).
-   `LESSON.md`: A detailed lesson explaining the concepts and code.

## How to Run

1.  **Generate Data**:
    ```bash
    python generate_data.py
    ```
    This creates the source CSV files in the `data/` directory.

2.  **Run Inmon ETL**:
    ```bash
    python inmon_etl.py
    ```
    This creates `inmon_edw.db` (SQLite database).

3.  **Run Kimball ETL**:
    ```bash
    python kimball_etl.py
    ```
    This creates `kimball_dw.db` (SQLite database).

## Comparison Summary

| Feature | Inmon (CIF) | Kimball (Dimensional) |
| :--- | :--- | :--- |
| **Primary Goal** | Enterprise-wide consistency | Query performance & Usability |
| **Data Model** | Normalized (3NF) | Denormalized (Star Schema) |
| **Approach** | Top-Down (EDW first) | Bottom-Up (Data Marts first) |
| **Complexity** | High (ETL is simple, Query is hard) | Low (ETL is complex, Query is simple) |
| **Time to Value** | Slow | Fast |

For a full explanation, please read [LESSON.md](LESSON.md).
