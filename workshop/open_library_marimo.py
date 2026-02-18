import marimo
import dlt
import ibis
import pandas

app = marimo.App()


@app.cell
def _():
    import marimo as mo
    import dlt
    import ibis

    return mo, dlt, ibis


@app.cell
def _(mo):
    title = mo.md("## Top 10 authors by book count")
    return (title,)


@app.cell
def _(dlt, mo):
    pipeline = dlt.pipeline(pipeline_name="open_library_pipeline", destination="duckdb")
    dataset = pipeline.dataset()
    dataset_name = pipeline.dataset_name
    ibis_connection = dataset.ibis()
    status = mo.md(f"Loaded dataset **{dataset_name}** from `open_library_pipeline`.")
    return pipeline, dataset, dataset_name, ibis_connection, status


@app.cell
def _(ibis_connection, dataset_name, ibis):
    authors = ibis_connection.table("books__authors", database=dataset_name)
    top_authors = (
        authors.group_by(authors.name)
        .aggregate(book_count=authors.name.count())
        .order_by(ibis.desc("book_count"))
        .limit(10)
    )
    top_authors_df = top_authors.execute()
    return authors, top_authors, top_authors_df


@app.cell
def _(mo, top_authors_df):
    table = mo.ui.table(top_authors_df)
    table
    return (table,)


if __name__ == "__main__":
    app.run()

