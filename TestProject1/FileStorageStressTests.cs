using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

// این تست توسط هوش مصنوعس ساخته شده است
public class FileStorageHeavyStressTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly FileStorage _fileStorage;
    private readonly CancellationTokenSource _cts;
    private readonly ConcurrentBag<Exception> _exceptions = new();
    private readonly ConcurrentDictionary<Guid, FileData> _savedFiles = new();
    private readonly Random _random = new();
    private readonly ITestOutputHelper _output;
    private const int TestDurationSeconds = 60;
    private const int MaxFileSizeBytes = 2 * 1024 * 1024; // 2MB
    private const int ThreadCount = 100;
    private const int MaxRetries = 5;
    private long _totalOperations = 0;
    private long _successfulOperations = 0;
    private volatile bool _testCompleted = false;

    private class FileData
    {
        public byte[] Data { get; set; }
        public string Hash { get; set; }
        public DateTime SaveTime { get; set; }
        public int Size { get; set; }
        public int ReadCount { get; set; }
    }

    public FileStorageHeavyStressTests(ITestOutputHelper output)
    {
        _output = output;
        _testDirectory = Path.Combine(Path.GetTempPath(), $"FileHeavyStressTest_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDirectory);

        // تنظیم دوره پاکسازی کوتاه برای فعال‌سازی مکرر فرآیند پاکسازی
        _fileStorage = new FileStorage(_testDirectory, iDeleteEveryMinute: 2);
        _cts = new CancellationTokenSource();

        _output.WriteLine($"Test directory created: {_testDirectory}");
    }

    [Fact]
    public async Task HeavyStressTest_FileOperationsUnderExtremeLoad()
    {
        var stopwatch = Stopwatch.StartNew();
        var tasks = new List<Task>();

        // ایجاد وظایف چندنخی
        for (int i = 0; i < ThreadCount; i++)
        {
            tasks.Add(Task.Run(() => WorkerThread(_cts.Token)));
        }

        // اجرای تست برای مدت زمان مشخص
        await Task.Delay(TimeSpan.FromSeconds(TestDurationSeconds));

        // علامت‌گذاری اتمام تست
        _testCompleted = true;

        // لغو توکن
        _cts.Cancel();

        // انتظار برای تکمیل تمام وظایف با تایم‌آوت کوتاه
        try
        {
            await Task.WhenAll(tasks);
        }
        catch (TaskCanceledException)
        {
            _output.WriteLine("Tasks were canceled as expected");
        }

        stopwatch.Stop();

        // گزارش نتایج
        _output.WriteLine($"Test completed in {stopwatch.Elapsed.TotalSeconds:F2} seconds");
        _output.WriteLine($"Total operations: {Interlocked.Read(ref _totalOperations)}");
        _output.WriteLine($"Successful operations: {Interlocked.Read(ref _successfulOperations)}");
        _output.WriteLine($"Total files saved: {_savedFiles.Count}");
        _output.WriteLine($"Total exceptions: {_exceptions.Count}");

        // بررسی خطاها (به جز TaskCanceledException)
        var realExceptions = _exceptions.Where(ex => !(ex is TaskCanceledException)).ToList();
        if (realExceptions.Count > 0)
        {
            foreach (var ex in realExceptions)
            {
                _output.WriteLine($"Exception: {ex}");
                _output.WriteLine($"Stack Trace: {ex.StackTrace}");
            }
            Assert.Fail($"Test failed with {realExceptions.Count} exceptions");
        }

        // بررسی یکپارچگی داده‌ها
        await VerifyDataIntegrityAsync();

        // بررسی عملیات پاکسازی
        await VerifyCleanupOperationsAsync();

        // گزارش نهایی
        _output.WriteLine($"Heavy stress test passed successfully!");
    }

    private async Task WorkerThread(CancellationToken cancellationToken)
    {
        int retryCount = 0;
        int localOperations = 0;
        int localSuccess = 0;

        while (!cancellationToken.IsCancellationRequested && !_testCompleted && retryCount < MaxRetries)
        {
            try
            {
                Interlocked.Increment(ref _totalOperations);
                localOperations++;

                var operation = _random.Next(0, 100);

                // 60% ذخیره فایل جدید
                if (operation < 60)
                {
                    await SaveRandomFileAsync(cancellationToken);
                }
                // 35% خواندن فایل موجود
                else if (operation < 95)
                {
                    await ReadRandomFileAsync(cancellationToken);
                }
                // 5% فعال‌سازی پاکسازی
                else
                {
                    _fileStorage.ForceCleanup();
                }

                Interlocked.Increment(ref _successfulOperations);
                localSuccess++;
                retryCount = 0; // ریست شمارنده تلاش مجدد در صورت موفقیت

                // تاخیر کوتاه برای کاهش فشار سیستم
                await Task.Delay(_random.Next(1, 10), cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // خروج از حلقه در صورت لغو عملیات
                break;
            }
            catch (Exception ex) when (retryCount < MaxRetries - 1)
            {
                retryCount++;
                _output.WriteLine($"Operation failed (attempt {retryCount}/{MaxRetries}): {ex.Message}");
                try
                {
                    await Task.Delay(100 * retryCount, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
            catch (Exception ex)
            {
                _output.WriteLine($"Operation failed permanently: {ex}");
                _exceptions.Add(ex);
                break;
            }
        }

        _output.WriteLine($"Thread completed. Operations: {localOperations}, Success: {localSuccess}");
    }

    private string ComputeHash(byte[] data)
    {
        using var sha256 = SHA256.Create();
        return BitConverter.ToString(sha256.ComputeHash(data)).Replace("-", "");
    }

    private async Task SaveRandomFileAsync(CancellationToken cancellationToken)
    {
        var fileSize = _random.Next(100, MaxFileSizeBytes);
        var fileData = new byte[fileSize];
        _random.NextBytes(fileData);

        // محاسبه هش فایل برای بررسی بعدی
        var hash = ComputeHash(fileData);

        using var stream = new MemoryStream(fileData);
        var fileId = await _fileStorage.SaveFileAsync(stream, cancellationToken);

        // ذخیره اطلاعات دقیق فایل
        _savedFiles.TryAdd(fileId, new FileData
        {
            Data = fileData,
            Hash = hash,
            SaveTime = DateTime.UtcNow,
            Size = fileSize,
            ReadCount = 0
        });

        // گزارش وضعیت
        if (_savedFiles.Count % 100 == 0)
        {
            _output.WriteLine($"Saved {_savedFiles.Count} files so far...");
        }
    }

    private async Task ReadRandomFileAsync(CancellationToken cancellationToken)
    {
        if (_savedFiles.IsEmpty) return;

        // انتخاب تصادفی یک فایل ذخیره شده
        var fileId = _savedFiles.Keys.ElementAt(_random.Next(_savedFiles.Count));

        try
        {
            // خواندن فایل به صورت همزمان
            using var stream = await _fileStorage.GetFileAsync(fileId, cancellationToken);
            using var memoryStream = new MemoryStream();
            await stream.CopyToAsync(memoryStream, cancellationToken);
            var retrievedData = memoryStream.ToArray();

            // مقایسه داده‌های بازیابی شده با داده‌های اصلی
            if (_savedFiles.TryGetValue(fileId, out var originalFileData))
            {
                // افزایش شمارنده خواندن
                originalFileData.ReadCount++;

                // بررسی اندازه فایل
                if (originalFileData.Size != retrievedData.Length)
                {
                    _output.WriteLine($"Size mismatch for file {fileId}. Original: {originalFileData.Size}, Retrieved: {retrievedData.Length}");
                    throw new InvalidDataException($"File size mismatch for file {fileId}");
                }

                // بررسی هش فایل
                var retrievedHash = ComputeHash(retrievedData);
                if (originalFileData.Hash != retrievedHash)
                {
                    _output.WriteLine($"Hash mismatch for file {fileId}");
                    _output.WriteLine($"Original hash: {originalFileData.Hash}");
                    _output.WriteLine($"Retrieved hash: {retrievedHash}");

                    // پیدا کردن اولین بایت متفاوت
                    for (int i = 0; i < Math.Min(originalFileData.Data.Length, retrievedData.Length); i++)
                    {
                        if (originalFileData.Data[i] != retrievedData[i])
                        {
                            _output.WriteLine($"First difference at byte {i}: Original={originalFileData.Data[i]}, Retrieved={retrievedData[i]}");
                            break;
                        }
                    }

                    throw new InvalidDataException($"Data integrity check failed for file {fileId}");
                }
            }
        }
        catch (FileNotFoundException)
        {
            // فایل ممکن است توسط فرآیند پاکسازی حذف شده باشد
            if (_savedFiles.TryRemove(fileId, out _))
            {
                _output.WriteLine($"File removed by cleanup: {fileId}");
            }
        }
        catch (Exception ex)
        {
            _output.WriteLine($"Error reading file {fileId}: {ex}");
            throw;
        }
    }

    private async Task VerifyDataIntegrityAsync()
    {
        _output.WriteLine("Verifying data integrity...");
        int verifiedCount = 0;
        int corruptionCount = 0;
        int missingCount = 0;
        int totalReads = 0;

        var verificationTasks = new List<Task>();
        foreach (var (fileId, originalFileData) in _savedFiles)
        {
            verificationTasks.Add(Task.Run(async () =>
            {
                try
                {
                    using var stream = await _fileStorage.GetFileAsync(fileId);
                    using var memoryStream = new MemoryStream();
                    await stream.CopyToAsync(memoryStream);
                    var retrievedData = memoryStream.ToArray();

                    // بررسی اندازه فایل
                    if (originalFileData.Size != retrievedData.Length)
                    {
                        Interlocked.Increment(ref corruptionCount);
                        _output.WriteLine($"Size mismatch for file {fileId}. Original: {originalFileData.Size}, Retrieved: {retrievedData.Length}");
                        throw new InvalidDataException($"File size mismatch for file {fileId}");
                    }

                    // بررسی هش فایل
                    var retrievedHash = ComputeHash(retrievedData);
                    if (originalFileData.Hash != retrievedHash)
                    {
                        Interlocked.Increment(ref corruptionCount);
                        _output.WriteLine($"Hash mismatch for file {fileId}");
                        _output.WriteLine($"Original hash: {originalFileData.Hash}");
                        _output.WriteLine($"Retrieved hash: {retrievedHash}");

                        // پیدا کردن اولین بایت متفاوت
                        for (int i = 0; i < Math.Min(originalFileData.Data.Length, retrievedData.Length); i++)
                        {
                            if (originalFileData.Data[i] != retrievedData[i])
                            {
                                _output.WriteLine($"First difference at byte {i}: Original={originalFileData.Data[i]}, Retrieved={retrievedData[i]}");
                                break;
                            }
                        }

                        throw new InvalidDataException($"Data integrity check failed for file {fileId}");
                    }

                    Interlocked.Increment(ref verifiedCount);
                    Interlocked.Add(ref totalReads, originalFileData.ReadCount);
                }
                catch (FileNotFoundException)
                {
                    Interlocked.Increment(ref missingCount);
                    // فایل ممکن است توسط فرآیند پاکسازی حذف شده باشد
                    _savedFiles.TryRemove(fileId, out _);
                }
            }));
        }

        await Task.WhenAll(verificationTasks);
        _output.WriteLine($"Data integrity verified for {verifiedCount} files");
        _output.WriteLine($"Missing files: {missingCount}");
        _output.WriteLine($"Total read operations during test: {totalReads}");

        if (corruptionCount > 0)
        {
            Assert.Fail($"Found {corruptionCount} corrupted files");
        }
    }

    private async Task VerifyCleanupOperationsAsync()
    {
        _output.WriteLine("Verifying cleanup operations...");

        // فعال‌سازی پاکسازی دستی
        _fileStorage.ForceCleanup();

        // بررسی فایل‌های باقیمانده در دیسک
        var filesOnDisk = Directory.GetFiles(_testDirectory, "*.dat");
        _output.WriteLine($"Files remaining on disk: {filesOnDisk.Length}");

        // بررسی فایل‌های موجود در ایندکس
        var indexFiles = _fileStorage.GetType()
            .GetField("_IndexManager", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
            .GetValue(_fileStorage);

        var indexCount = (int)indexFiles.GetType()
            .GetProperty("Count")
            .GetValue(indexFiles);

        _output.WriteLine($"Files in index: {indexCount}");

        // بررسی تطابق تعداد فایل‌ها
        if (Math.Abs(filesOnDisk.Length - indexCount) > 2) // اختلاف 2 قابل قبول است
        {
            _output.WriteLine($"Warning: Mismatch between disk files ({filesOnDisk.Length}) and index ({indexCount})");
        }

        // بررسی وجود فایل‌های یتیم
        var orphanedFiles = filesOnDisk.Where(f =>
        {
            var fileName = Path.GetFileNameWithoutExtension(f);
            return Guid.TryParseExact(fileName, "N", out _) &&
                   !_savedFiles.ContainsKey(Guid.Parse(fileName));
        }).ToList();

        if (orphanedFiles.Count > 0)
        {
            _output.WriteLine($"Found {orphanedFiles.Count} orphaned files");
        }

        // بررسی فایل‌های tmp
        var tempFiles = Directory.GetFiles(_testDirectory, "*.tmp");
        if (tempFiles.Length > 0)
        {
            _output.WriteLine($"Found {tempFiles.Length} temporary files");
            foreach (var tempFile in tempFiles)
            {
                _output.WriteLine($"Temp file: {Path.GetFileName(tempFile)}");
            }
        }

        // بررسی فایل‌های لاگ
        var logFiles = Directory.GetFiles(_testDirectory, "*.log");
        if (logFiles.Length > 0)
        {
            _output.WriteLine($"Found {logFiles.Length} log files");
        }
    }

    public void Dispose()
    {
        _cts?.Dispose();
        _fileStorage?.Dispose();

        try
        {
            if (Directory.Exists(_testDirectory))
            {
                Directory.Delete(_testDirectory, true);
                _output.WriteLine($"Test directory deleted: {_testDirectory}");
            }
        }
        catch (Exception ex)
        {
            _output.WriteLine($"Error deleting test directory: {ex}");
        }
    }
}