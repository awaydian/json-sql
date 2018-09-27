<?php
namespace JSONSQL;

class Storage {
    private $lockPath;

    function __construct($lockPath) {
        $this->lockPath = $lockPath;
    }

    
    function lockFile($filePath, $attempts = 30, $interval = 0.1)
    {
        $interval = $interval * 1000000;
        try {
            $lockFilePath = $this->getLockFilePath($filePath);
            /** @noinspection PhpUndefinedMethodInspection */
            // \Illuminate\Support\Facades\Log::info("Try lock file {$filePath}, retry 0.");
            if (file_exists($lockFilePath) && !is_dir($lockFilePath)) {
                // touch命令可能导致创建了一个与lock文件夹同名的文件，这里直接删除
                @unlink($lockFilePath);
            }
            $success = false;
            try {
                $success = !file_exists($lockFilePath) && mkdir($lockFilePath);
            } catch (Throwable $e) {
                if (!file_exists($lockFilePath)) {
                    throw $e;
                }
            }
            $currentAttempts = 1;
            if (!$success) {
                /** @noinspection PhpUndefinedMethodInspection */
                // \Illuminate\Support\Facades\Log::info("Lock file {$lockFilePath} exists, try touch.");
                try {
                    touch($lockFilePath, 0);
                } catch (Throwable $e) {
                    // ignore
                }
                usleep($interval);
            }
            while (!$success && $attempts > 0 && $currentAttempts < $attempts) {
                /** @noinspection PhpUndefinedMethodInspection */
                // \Illuminate\Support\Facades\Log::info("Try lock file {$filePath}, retry {$currentAttempts}.");
                if (file_exists($lockFilePath) && !is_dir($lockFilePath)) {
                    // touch命令可能导致创建了一个与lock文件夹同名的文件，这里直接删除
                    @unlink($lockFilePath);
                }
                try {
                    $success = !file_exists($lockFilePath) && mkdir($lockFilePath);
                } catch (Throwable $e) {
                    if (!file_exists($lockFilePath)) {
                        throw $e;
                    }
                }
                $currentAttempts++;
                /** @noinspection PhpUndefinedMethodInspection */
                // \Illuminate\Support\Facades\Log::info("Lock file {$lockFilePath} exists, try touch.");
                try {
                    touch($lockFilePath, 0);
                } catch (Throwable $e) {
                    // ignore
                }
                usleep($interval);
            }
            if ($success) {
                /** @noinspection PhpUndefinedMethodInspection */
                // \Illuminate\Support\Facades\Log::info("File {$filePath} locked before retry {$currentAttempts}.");
            }
            return $success;
        } catch (Throwable $e) {
            /** @noinspection PhpUndefinedMethodInspection */
            // \Illuminate\Support\Facades\Log::warning("Failed to lock file {$filePath}, error info: {$e->getMessage()}.");
            /** @noinspection PhpUndefinedMethodInspection */
            // \Illuminate\Support\Facades\Log::info("Stack trace of lock_file\n" . $e->getTraceAsString());
            return false;
        }
    }

    function unlockFile($filePath, $attempts = 0, $interval = 0.1)
    {
        $interval = $interval * 1000000;
        try {
            $lockFilePath = $this->getLockFilePath($filePath);
            $success = rmdir($lockFilePath) || !is_file_locked($filePath);
            /** @noinspection PhpUndefinedMethodInspection */
            // \Illuminate\Support\Facades\Log::info("Try unlock file {$filePath}, retry 0.");
            $currentAttempts = 1;
            while (!$success && $attempts > 0 && $currentAttempts < $attempts) {
                usleep($interval);
                /** @noinspection PhpUndefinedMethodInspection */
                // \Illuminate\Support\Facades\Log::info("Try unlock file {$filePath}, retry {$currentAttempts}.");
                $success = rmdir($lockFilePath) || !is_file_locked($filePath);
                $currentAttempts++;
            }
            if ($success) {
                /** @noinspection PhpUndefinedMethodInspection */
                // \Illuminate\Support\Facades\Log::info("File {$filePath} unlocked before retry {$currentAttempts}.");
            }
            return $success;
        } catch (Throwable $e) {
            /** @noinspection PhpUndefinedMethodInspection */
            // \Illuminate\Support\Facades\Log::warning("Failed to unlock file {$filePath}, error info: {$e->getMessage()}.");
            /** @noinspection PhpUndefinedMethodInspection */
            // \Illuminate\Support\Facades\Log::info("Stack trace of unlock_file\n" . $e->getTraceAsString());
            return false;
        }
    }

    // private function getLockFilePath($filePath)
    function getLockFilePath($filePath)
    {
        $clusterCfgPath = $this->lockPath;
        // if (starts_with($filePath, $clusterCfgPath)) {
        //     return env('CFG_LOCK_PATH', base_path(DFT_CFG_LOCK_PATH)) . '/' . md5($filePath) . '(' . str_replace('/', '>', $filePath) . ')' . '.lock';
        // } else {
        //     return env('CFG_LOCK_PATH', base_path(DFT_CFG_LOCK_PATH)) . '/' . md5($filePath) . '(' . str_replace('/', '>', $filePath) . ')@' . \App\Providers\AppServiceProvider::getClusterService()->getLocalHostUid() . '.lock';
        // }

        return $this->lockPath . md5($filePath) . md5($filePath) . '(' . str_replace('/', '>', $filePath) . ')' . '.lock';

        return $this->lockPath . md5($filePath) . md5($filePath) . '(' . str_replace('/', '>', $filePath) . ')@' . '__host_uuid__' . '.lock';
    }
}