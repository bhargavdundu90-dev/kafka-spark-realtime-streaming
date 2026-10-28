"""spark-submit entry point (spark-submit cannot run a module with relative imports).

Local:  PYTHONPATH=src spark-submit --packages ... jobs/stream_job.py
AWS:    ship src/txnstream as a zip via --py-files (see docs/aws-deployment.md)
"""
from txnstream.job import main

if __name__ == "__main__":
    main()
