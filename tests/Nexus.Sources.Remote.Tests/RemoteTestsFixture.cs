using System.Diagnostics;
using Xunit;

namespace Nexus.Sources.Tests;

public class RemoteTestsFixture : IAsyncLifetime, IDisposable
{
    private Process? _buildProcess_dotnet;

    private Process? _runProcess_dotnet;

    private Process? _runProcess_python;

    private readonly SemaphoreSlim _semaphoreBuild = new(0, 1);

    private readonly SemaphoreSlim _semaphoreRun = new(0, 1);

    private bool _disposed;

    public Task InitializeAsync()
    {
        var dotnetTask = RunDotnetAgent();
        var pythonTask = RunPythonAgent();

        return Task.WhenAll(dotnetTask, pythonTask);
    }

    public Task DisposeAsync()
    {
        Dispose();
        return Task.CompletedTask;
    }

    private async Task RunDotnetAgent()
    {
        /* Why not `dotnet run`? Because it spawns a child process for which
         * we do not know the process ID and so we cannot kill it.
         */

        // Build Nexus.Agent
        var psi_build = new ProcessStartInfo("bash")
        {
            /* Why `sleep infinity`? Because the test debugger seems to stop whenever a child process stops */
            Arguments = "-c \"dotnet build ../../../../src/agent/dotnet/agent.csproj && sleep infinity\"",
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };

        _buildProcess_dotnet = new Process
        {
            StartInfo = psi_build,
            EnableRaisingEvents = true
        };

        var success = false;

        _buildProcess_dotnet.OutputDataReceived += (sender, e) =>
        {
            if (e.Data is not null && e.Data.Contains("Build succeeded"))
            {
                success = true;
                _semaphoreBuild.Release();
            }
        };

        _buildProcess_dotnet.ErrorDataReceived += (sender, e) =>
        {
            success = false;
            _semaphoreBuild.Release();
        };

        _buildProcess_dotnet.Start();
        _buildProcess_dotnet.BeginOutputReadLine();
        _buildProcess_dotnet.BeginErrorReadLine();

        await _semaphoreBuild.WaitAsync(TimeSpan.FromMinutes(1));

        if (!success)
            throw new Exception("Unable to build Nexus.Agent.");

        // Run Nexus.Agent
        var psi_run = new ProcessStartInfo("dotnet")
        {
            Arguments = $"../../../artifacts/bin/agent/debug/Nexus.Agent.dll",
            WorkingDirectory="../../../../src/agent/dotnet",
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };

        psi_run.Environment["NEXUSAGENT_PATHS__CONFIG"] = "../../../.nexus-agent-dotnet/config";
        psi_run.Environment["NEXUSAGENT_SYSTEM__JSONRPCLISTENPORT"] = "60000";

        _runProcess_dotnet = new Process
        {
            StartInfo = psi_run,
            EnableRaisingEvents = true
        };

        _runProcess_dotnet.OutputDataReceived += (sender, e) =>
        {
            // File.AppendAllText("/home/vincent/Downloads/output2.txt", e.Data + Environment.NewLine);

            if (e.Data is not null && e.Data.Contains("Now listening on"))
            {
                success = true;
                _semaphoreRun.Release();
            }
        };

        _runProcess_dotnet.ErrorDataReceived += (sender, e) =>
        {
            // File.AppendAllText("/home/vincent/Downloads/error2.txt", e.Data + Environment.NewLine);

            var oldSuccess = success;
            success = false;

            if (oldSuccess)
                _semaphoreRun.Release();
        };

        _runProcess_dotnet.Start();
        _runProcess_dotnet.BeginOutputReadLine();
        _runProcess_dotnet.BeginErrorReadLine();

        await _semaphoreRun.WaitAsync(TimeSpan.FromMinutes(1));

        if (!success)
            throw new Exception("Unable to launch Nexus.Agent (dotnet).");
    }

    private async Task RunPythonAgent()
    {
        var psi_run = new ProcessStartInfo("bash")
        {
            Arguments = $"-c \"source ../../../.venv/bin/activate; fastapi run main.py --port 60002\"",
            WorkingDirectory="../../../../src/agent/python",
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };

        psi_run.Environment["PYTHONPATH"] = "../../remoting/python";
        psi_run.Environment["NEXUSAGENT_PATHS__CONFIG"] = "../../../.nexus-agent-python/config";
        psi_run.Environment["NEXUSAGENT_PATHS__PACKAGES"] = "../../../.nexus-agent-python/packages";
        psi_run.Environment["NEXUSAGENT_SYSTEM__JSONRPCLISTENPORT"] = "60001";

        _runProcess_python = new Process
        {
            StartInfo = psi_run,
            EnableRaisingEvents = true
        };

        var success = false;

        _runProcess_python.OutputDataReceived += (sender, e) =>
        {
            // File.AppendAllText("/home/vincent/Downloads/output.txt", e.Data + Environment.NewLine);

            if (e.Data is not null && e.Data.Contains("Application startup complete."))
            {
                success = true;
                _semaphoreRun.Release();
            }
        };

        _runProcess_python.ErrorDataReceived += (sender, e) =>
        {
            // File.AppendAllText("/home/vincent/Downloads/error.txt", e.Data + Environment.NewLine);

            if (e.Data is not null && e.Data.Contains("Application startup complete."))
            {
                success = true;
                _semaphoreRun.Release();
            }

            if (e.Data is not null && e.Data.ToLower().Contains("error"))
            {
                var oldSuccess = success;
                success = false;

                if (oldSuccess)
                    _semaphoreRun.Release();
            }
        };

        _runProcess_python.Start();
        _runProcess_python.BeginOutputReadLine();
        _runProcess_python.BeginErrorReadLine();

        await _semaphoreRun.WaitAsync(TimeSpan.FromMinutes(1));

        if (!success)
            throw new Exception("Unable to launch Nexus.Agent (python).");
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        KillProcess(_buildProcess_dotnet);
        KillProcess(_runProcess_dotnet);
        KillProcess(_runProcess_python);
    }

    private static void KillProcess(Process? process)
    {
        if (process is null || process.HasExited)
            return;

        process.Kill();
    }
}
