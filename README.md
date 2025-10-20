This project implements a distributed data analytics pipeline for large-scale wine quality feature analysis. The system integrates MapReduce batch processing, Spark-based distributed computation, synthetic big data generation, and a production-style Streamlit monitoring dashboard. The pipeline demonstrates real-world data engineering workflow design including distributed aggregation, feature ranking, and visualization layers.

## Project Structure

- `data/`: Contains input and output datasets.
- `mapreduce/`: Contains MapReduce implementation.
- `spark/`: Contains Spark implementation.
- `app/`: Contains Streamlit dashboard.
- `requirements.txt`: Contains project dependencies.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Generate synthetic big data:
```bash
python generate_big_data.py
```

3. Run MapReduce pipeline:
```bash
python mapreduce/mapreduce_analysis.py data/generated_big_wine_dataset.csv > output/mapreduce_results.json
```

4. Run Spark pipeline:
```bash
python spark/spark_analysis.py
```

5. Run Streamlit dashboard:
```bash
streamlit run app/app.py
```

## Output

- `output/mapreduce_results.json`: Contains MapReduce output.
- `output/spark_feature_analysis.csv`: Contains Spark output.
- `output/streamlit_dashboard.html`: Contains Streamlit dashboard.

