import argparse, asyncio
from app.pipeline.step_worker import StepWorker

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("step_type"); ap.add_argument("--once", action="store_true")
    args=ap.parse_args(); w=StepWorker(args.step_type)
    asyncio.run(w.run_once() if args.once else w.run_forever())

if __name__ == "__main__": main()
