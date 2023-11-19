import cProfile
import pstats
import io

from src.task_1.equity_tick import main

if __name__ == '__main__':
    pr = cProfile.Profile()
    pr.enable()
    main()  # call the function
    pr.disable()
    s = io.StringIO()
    sortby = 'cumulative'
    ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    ps.print_stats()
    print(s.getvalue())