using System.Collections.Concurrent;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;

class VanishImmediateScheduler : IScheduler
{
    public DateTimeOffset Now => DateTimeOffset.Now;

    public IDisposable Schedule<TState>(TState state, Func<IScheduler, TState, IDisposable> action)
    {
        Console.WriteLine(
            $"[T:{Environment.CurrentManagedThreadId}] VanishImmediateScheduler Scheduled in time: {Now}"
        );

        action(this, state);
        return Disposable.Empty;
    }

    public IDisposable Schedule<TState>(
        TState state,
        TimeSpan dueTime,
        Func<IScheduler, TState, IDisposable> action
    )
    {
        Console.WriteLine(
            $"[T:{Environment.CurrentManagedThreadId}] VanishImmediateScheduler Scheduled in time: {Now}"
        );

        if (dueTime > TimeSpan.Zero)
        {
            Thread.Sleep(dueTime);
        }
        action(this, state);
        return Disposable.Empty;
    }

    public IDisposable Schedule<TState>(
        TState state,
        DateTimeOffset dueTime,
        Func<IScheduler, TState, IDisposable> action
    )
    {
        Console.WriteLine(
            $"[T:{Environment.CurrentManagedThreadId}] VanishImmediateScheduler Scheduled in time: {Now}"
        );

        var delay = dueTime - Now;
        if (delay > TimeSpan.Zero)
        {
            System.Threading.Thread.Sleep(delay);
        }
        action(this, state);
        return Disposable.Empty;
    }
}

class VanishCurrentThreadScheduler : IScheduler
{
    private readonly ThreadLocal<Queue<Action>> _queue = new(() => new Queue<Action>());
    private readonly ThreadLocal<bool> _isRunning = new(() => false);

    public DateTimeOffset Now => DateTimeOffset.Now;

    public IDisposable Schedule<TState>(TState state, Func<IScheduler, TState, IDisposable> action)
    {
        Console.WriteLine(
            $"[T:{Environment.CurrentManagedThreadId}] VanishCurrentThreadScheduler Scheduled immediately"
        );

        var cancelled = false;
        var work = new Action(() =>
        {
            if (!cancelled)
                action(this, state);
        });

        _queue.Value!.Enqueue(work);
        RunQueue();

        return Disposable.Create(() => cancelled = true);
    }

    public IDisposable Schedule<TState>(
        TState state,
        TimeSpan dueTime,
        Func<IScheduler, TState, IDisposable> action
    )
    {
        Console.WriteLine(
            $"[T:{Environment.CurrentManagedThreadId}] VanishCurrentThreadScheduler Scheduled with delay: {dueTime}"
        );

        var cancelled = false;
        var work = new Action(() =>
        {
            if (!cancelled)
            {
                if (dueTime > TimeSpan.Zero)
                    Thread.Sleep(dueTime);
                action(this, state);
            }
        });

        _queue.Value!.Enqueue(work);
        RunQueue();

        return Disposable.Create(() => cancelled = true);
    }

    public IDisposable Schedule<TState>(
        TState state,
        DateTimeOffset dueTime,
        Func<IScheduler, TState, IDisposable> action
    )
    {
        var delay = dueTime - Now;
        return Schedule(state, delay, action);
    }

    private void RunQueue()
    {
        if (_isRunning.Value!)
            return;

        _isRunning.Value = true;
        try
        {
            while (_queue.Value!.Count > 0)
            {
                var work = _queue.Value.Dequeue();
                work();
            }
        }
        finally
        {
            _isRunning.Value = false;
        }
    }
}

class VanishEventLoopScheduler : IScheduler, IDisposable
{
    private readonly Thread _thread;
    private readonly BlockingCollection<WorkItem> _queue = new();
    private volatile bool _disposed = false;

    public DateTimeOffset Now => DateTimeOffset.Now;

    public VanishEventLoopScheduler()
    {
        _thread = new Thread(EventLoop) { IsBackground = true, Name = "VanishEventLoopScheduler" };
        _thread.Start();
    }

    public IDisposable Schedule<TState>(TState state, Func<IScheduler, TState, IDisposable> action)
    {
        Console.WriteLine(
            $"[T:{Environment.CurrentManagedThreadId}] VanishEventLoopScheduler Scheduled immediately"
        );

        var workItem = new WorkItem(() => action(this, state));
        _queue.Add(workItem);
        return workItem;
    }

    public IDisposable Schedule<TState>(
        TState state,
        TimeSpan dueTime,
        Func<IScheduler, TState, IDisposable> action
    )
    {
        Console.WriteLine(
            $"[T:{Environment.CurrentManagedThreadId}] VanishEventLoopScheduler Scheduled with delay: {dueTime}"
        );

        var workItem = new WorkItem(() => action(this, state), dueTime);
        _queue.Add(workItem);
        return workItem;
    }

    public IDisposable Schedule<TState>(
        TState state,
        DateTimeOffset dueTime,
        Func<IScheduler, TState, IDisposable> action
    )
    {
        var delay = dueTime - Now;
        return Schedule(state, delay, action);
    }

    private void EventLoop()
    {
        while (!_disposed)
        {
            try
            {
                if (_queue.TryTake(out var workItem, 100))
                {
                    if (workItem.DueTime > TimeSpan.Zero)
                        Thread.Sleep(workItem.DueTime);

                    if (!workItem.IsCancelled)
                        workItem.Action();
                }
            }
            catch when (_disposed)
            {
                break;
            }
        }
    }

    public void Dispose()
    {
        _disposed = true;
        _queue.CompleteAdding();
        _thread.Join(1000);
        _queue.Dispose();
    }

    private class WorkItem : IDisposable
    {
        public Action Action { get; }
        public TimeSpan DueTime { get; }
        public bool IsCancelled { get; private set; }

        public WorkItem(Action action, TimeSpan dueTime = default)
        {
            Action = action;
            DueTime = dueTime;
        }

        public void Dispose() => IsCancelled = true;
    }
}

class VanishDefaultScheduler : IScheduler
{
    public DateTimeOffset Now => DateTimeOffset.Now;

    public IDisposable Schedule<TState>(TState state, Func<IScheduler, TState, IDisposable> action)
    {
        Console.WriteLine(
            $"[T:{Environment.CurrentManagedThreadId}] VanishDefaultScheduler Scheduled on ThreadPool"
        );

        var cancelled = false;
        ThreadPool.QueueUserWorkItem(_ =>
        {
            if (!cancelled)
                action(this, state);
        });

        return Disposable.Create(() => cancelled = true);
    }

    public IDisposable Schedule<TState>(
        TState state,
        TimeSpan dueTime,
        Func<IScheduler, TState, IDisposable> action
    )
    {
        Console.WriteLine(
            $"[T:{Environment.CurrentManagedThreadId}] VanishDefaultScheduler Scheduled on ThreadPool with delay: {dueTime}"
        );

        var cancelled = false;
        var timer = new Timer(
            _ =>
            {
                if (!cancelled)
                    ThreadPool.QueueUserWorkItem(__ => action(this, state));
            },
            null,
            dueTime,
            Timeout.InfiniteTimeSpan
        );

        return Disposable.Create(() =>
        {
            cancelled = true;
            timer.Dispose();
        });
    }

    public IDisposable Schedule<TState>(
        TState state,
        DateTimeOffset dueTime,
        Func<IScheduler, TState, IDisposable> action
    )
    {
        var delay = dueTime - Now;
        if (delay <= TimeSpan.Zero)
            return Schedule(state, action);

        return Schedule(state, delay, action);
    }
}

class VanishNewThreadScheduler : IScheduler
{
    public DateTimeOffset Now => DateTimeOffset.Now;

    public IDisposable Schedule<TState>(TState state, Func<IScheduler, TState, IDisposable> action)
    {
        Console.WriteLine(
            $"[T:{Environment.CurrentManagedThreadId}] VanishNewThreadScheduler Creating new thread"
        );

        var cancelled = false;
        var thread = new Thread(() =>
        {
            if (!cancelled)
                action(this, state);
        })
        {
            IsBackground = true,
            Name = "VanishNewThreadScheduler",
        };

        thread.Start();
        return Disposable.Create(() => cancelled = true);
    }

    public IDisposable Schedule<TState>(
        TState state,
        TimeSpan dueTime,
        Func<IScheduler, TState, IDisposable> action
    )
    {
        Console.WriteLine(
            $"[T:{Environment.CurrentManagedThreadId}] VanishNewThreadScheduler Creating new thread with delay: {dueTime}"
        );

        var cancelled = false;
        var thread = new Thread(() =>
        {
            if (dueTime > TimeSpan.Zero)
                Thread.Sleep(dueTime);

            if (!cancelled)
                action(this, state);
        })
        {
            IsBackground = true,
            Name = "VanishNewThreadScheduler",
        };

        thread.Start();
        return Disposable.Create(() => cancelled = true);
    }

    public IDisposable Schedule<TState>(
        TState state,
        DateTimeOffset dueTime,
        Func<IScheduler, TState, IDisposable> action
    )
    {
        var delay = dueTime - Now;
        if (delay <= TimeSpan.Zero)
            return Schedule(state, action);

        return Schedule(state, delay, action);
    }
}

static class VanishRxExtensions
{
    /// <summary>
    /// Controls the scheduler on which the source observable is subscribed to.
    /// This affects where the subscription logic (OnSubscribe) runs.
    /// </summary>
    public static IObservable<T> VanishSubscribeOn<T>(
        this IObservable<T> source,
        IScheduler scheduler
    )
    {
        return Observable.Create<T>(observer =>
        {
            Console.WriteLine(
                $"[T:{Environment.CurrentManagedThreadId}] VanishSubscribeOn: Scheduling subscription on {scheduler.GetType().Name}"
            );

            var subscription = new SerialDisposable();

            var scheduledWork = scheduler.Schedule(() =>
            {
                Console.WriteLine(
                    $"[T:{Environment.CurrentManagedThreadId}] VanishSubscribeOn: Executing subscription on {scheduler.GetType().Name}"
                );

                subscription.Disposable = source.Subscribe(observer);
            });

            return new CompositeDisposable(subscription, scheduledWork);
        });
    }

    public static IObservable<T> VanishObserveOn<T>(
        this IObservable<T> source,
        IScheduler scheduler
    )
    {
        return Observable.Create<T>(observer =>
        {
            Console.WriteLine(
                $"[T:{Environment.CurrentManagedThreadId}] VanishObserveOn: Setting up observation on {scheduler.GetType().Name}"
            );

            return source.Subscribe(
                onNext: value =>
                {
                    scheduler.Schedule(() =>
                    {
                        Console.WriteLine(
                            $"[T:{Environment.CurrentManagedThreadId}] VanishObserveOn: Delivering OnNext({value}) on {scheduler.GetType().Name}"
                        );
                        observer.OnNext(value);
                    });
                },
                onError: error =>
                {
                    scheduler.Schedule(() =>
                    {
                        Console.WriteLine(
                            $"[T:{Environment.CurrentManagedThreadId}] VanishObserveOn: Delivering OnError on {scheduler.GetType().Name}"
                        );
                        observer.OnError(error);
                    });
                },
                onCompleted: () =>
                {
                    scheduler.Schedule(() =>
                    {
                        Console.WriteLine(
                            $"[T:{Environment.CurrentManagedThreadId}] VanishObserveOn: Delivering OnCompleted on {scheduler.GetType().Name}"
                        );
                        observer.OnCompleted();
                    });
                }
            );
        });
    }
}

static class Program
{
    public static void Main()
    {
        // Console.WriteLine("================================");
        // Console.WriteLine("=== Testing VanishDefaultScheduler ===");
        // TestScheduler(new VanishDefaultScheduler(), "Default");
        // Console.WriteLine("=== Testing VanishDefaultScheduler Done ===");
        // Console.WriteLine("================================\n\n\n");

        // Console.WriteLine("================================");
        // Console.WriteLine("=== Testing VanishNewThreadScheduler ===");
        // TestScheduler(new VanishNewThreadScheduler(), "NewThread");
        // Console.WriteLine("=== Testing VanishNewThreadScheduler Done ===");
        // Console.WriteLine("================================\n\n\n");

        // Console.WriteLine("================================");
        // Console.WriteLine("=== Testing VanishEventLoopScheduler ===");
        // using var eventLoopScheduler = new VanishEventLoopScheduler();
        // TestScheduler(eventLoopScheduler, "EventLoop");
        // Console.WriteLine("=== Testing VanishEventLoopScheduler Done ===");
        // Console.WriteLine("================================\n\n\n");

        // Console.WriteLine("================================");
        // Console.WriteLine("=== Testing VanishImmediateScheduler ===");
        // TestSchedulerWithSubscribeOn(
        //     new VanishImmediateScheduler(),
        //     new VanishEventLoopScheduler(),
        //     "Immediate"
        // );
        // Console.WriteLine("=== Testing VanishImmediateScheduler Done ===");
        // Console.WriteLine("================================\n\n\n");

        // Console.WriteLine("================================");
        // Console.WriteLine("=== Testing VanishCurrentThreadScheduler ===");
        // TestSchedulerWithSubscribeOn(
        //     new VanishCurrentThreadScheduler(),
        //     new VanishEventLoopScheduler(),
        //     "CurrentThread"
        // );
        // Console.WriteLine("=== Testing VanishCurrentThreadScheduler Done ===");
        // Console.WriteLine("================================\n\n\n");

        // Console.WriteLine("================================");
        // Console.WriteLine("=== Testing VanishImmediateScheduler ===");
        // TestScheduler(new VanishImmediateScheduler(), "Immediate");
        // Console.WriteLine("=== Testing VanishImmediateScheduler Done ===");
        // Console.WriteLine("================================\n\n\n");

        Console.WriteLine("================================");
        Console.WriteLine("=== Testing VanishCurrentThreadScheduler ===");
        TestScheduler(new VanishCurrentThreadScheduler(), "CurrentThread");
        Console.WriteLine("=== Testing VanishCurrentThreadScheduler Done ===");
        Console.WriteLine("================================\n\n\n");

        Console.WriteLine("\nPress any key to exit...");
        Console.ReadKey();
    }

    private static void TestScheduler(IScheduler scheduler, string name)
    {
        Console.WriteLine(
            $"Testing {name} Scheduler on thread {Environment.CurrentManagedThreadId}"
        );

        VanishInterval(TimeSpan.FromMilliseconds(1000), scheduler)
            .Take(5)
            .Subscribe(
                tick =>
                    Console.WriteLine(
                        $"[T:{Environment.CurrentManagedThreadId}] {name} Tick {tick}"
                    ),
                () => Console.WriteLine($"{name} Ticking completed\n")
            );

        Console.WriteLine($"{name} completed");

        // Give some time for async schedulers to complete
        Thread.Sleep(10000);
    }

    private static void TestSchedulerWithSubscribeOn(
        IScheduler scheduler,
        IScheduler subscribeOnScheduler,
        string name
    )
    {
        Console.WriteLine(
            $"Testing {name} Scheduler with SubscribeOn on {subscribeOnScheduler.GetType().Name}"
        );

        VanishInterval(TimeSpan.FromMilliseconds(1000), scheduler)
            .Take(5)
            .SubscribeOn(subscribeOnScheduler)
            .Subscribe(tick =>
                Console.WriteLine($"[T:{Environment.CurrentManagedThreadId}] {name} Tick {tick}")
            );

        Console.WriteLine($"{name} Ticking completed");

        // Give some time for async schedulers to complete
        Thread.Sleep(10000);
    }

    private static IObservable<long> VanishInterval(TimeSpan period, IScheduler scheduler)
    {
        return Observable.Create<long>(observer =>
        {
            var serialDisposable = new SerialDisposable();

            IDisposable ScheduleNext(IScheduler sch, long counter)
            {
                return sch.Schedule( // 调度器返回的IDisposable, 可能可以用于取消还未开始的任务
                    counter,
                    period,
                    (innerScheduler, current) =>
                    {
                        observer.OnNext(current);
                        serialDisposable.Disposable = ScheduleNext(innerScheduler, current + 1);
                        return Disposable.Empty; // 回调函数返回的Disposable.Empty表示回调函数已经执行完毕,没有需要处理的中间资源.
                    }
                );
            }

            serialDisposable.Disposable = ScheduleNext(scheduler, 0L);
            return serialDisposable;
        });
        /*
        * 调用栈1: Main() 
        * → VanishInterval() 
        * → ScheduleNext(scheduler, 0)
        * → scheduler.Schedule()  ← 立即返回(调度器的立即返回,对于ImmediateScheduler来说,他的立即返回是Sleep(1000)后再返回)
        * → 函数结束
        *
        * 调用栈2: 调度器线程
        * → 执行回调函数
        * → observer.OnNext(0)
        * → ScheduleNext(innerScheduler, 1)  ← 新的调用栈
        * → innerScheduler.Schedule()  ← 立即返回
        * → 回调函数结束
        */
    }
}
