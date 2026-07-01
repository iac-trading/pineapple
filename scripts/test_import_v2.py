import sys
import os
sys.path.append(os.path.join(os.getcwd(), 'services', '21_backtester', 'app'))
try:
    import main
    print("Import main success")
    print(f"run_strategy_19: {main.run_strategy_19}")
    print(f"run_strategy_214: {main.run_strategy_214}")
except Exception as e:
    print(f"Import failed: {e}")
    import traceback
    traceback.print_exc()
