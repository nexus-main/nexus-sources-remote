using System.Diagnostics;
using Xunit;

namespace Nexus.Sources.Tests;

public class RemoteTestsFixture : IAsyncLifetime
{
    private Process? _buildProcess_dotnet;

    private Process? _runProcess_dotnet;

    private Process? _runProcess_python;

    public Task InitializeAsync()
    {
        var dotnetTask = RunDotnetAgent();
        var pythonTask = RunPythonAgent();

        return Task.WhenAll(dotnetTask, pythonTask);
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
            Arguments = "-c \"dotnet build ../../../../src/agent/dotnet/agent.csproj && echo 'Build succeeded' && sleep infinity\"",
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };

        _buildProcess_dotnet = new Process
        {
            StartInfo = psi_build,
            EnableRaisingEvents = true
        };

        var buildCompleted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        _buildProcess_dotnet.OutputDataReceived += (sender, e) =>
        {
            if (e.Data is not null && e.Data.Contains("Build succeeded"))
                buildCompleted.TrySetResult(true);
        };

        _buildProcess_dotnet.ErrorDataReceived += (sender, e) =>
        {
            if (e.Data is not null)
                buildCompleted.TrySetResult(false);
        };

        _buildProcess_dotnet.Start();
        _buildProcess_dotnet.BeginOutputReadLine();
        _buildProcess_dotnet.BeginErrorReadLine();

        if (!await WaitForStartupAsync(buildCompleted.Task))
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
        psi_run.Environment["NEXUSAGENT_PATHS__PACKAGES"] = "../../../.nexus-agent-dotnet/packages";
        psi_run.Environment["NEXUSAGENT_SYSTEM__JSONRPCLISTENPORT"] = "60000";

        _runProcess_dotnet = new Process
        {
            StartInfo = psi_run,
            EnableRaisingEvents = true
        };

        var runCompleted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        _runProcess_dotnet.OutputDataReceived += (sender, e) =>
        {
            // File.AppendAllText("/home/vincent/Downloads/output2.txt", e.Data + Environment.NewLine);

            if (e.Data is not null && e.Data.Contains("Now listening on"))
                runCompleted.TrySetResult(true);
        };

        _runProcess_dotnet.ErrorDataReceived += (sender, e) =>
        {
            // File.AppendAllText("/home/vincent/Downloads/error2.txt", e.Data + Environment.NewLine);

            if (e.Data is not null && e.Data.Contains("error", StringComparison.OrdinalIgnoreCase))
                runCompleted.TrySetResult(false);
        };

        _runProcess_dotnet.Start();
        _runProcess_dotnet.BeginOutputReadLine();
        _runProcess_dotnet.BeginErrorReadLine();

        if (!await WaitForStartupAsync(runCompleted.Task))
            throw new Exception("Unable to launch Nexus.Agent (dotnet).");
    }

    private async Task RunPythonAgent()
    {
        var psi_run = new ProcessStartInfo("bash")
        {
            Arguments = $"-c \"source ../../../.venv/bin/activate; fastapi run main.py\"",
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

        var runCompleted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        _runProcess_python.OutputDataReceived += (sender, e) =>
        {
            // File.AppendAllText("/home/vincent/Downloads/output.txt", e.Data + Environment.NewLine);

            if (e.Data is not null && e.Data.Contains("Application startup complete."))
                runCompleted.TrySetResult(true);
        };

        _runProcess_python.ErrorDataReceived += (sender, e) =>
        {
            // File.AppendAllText("/home/vincent/Downloads/error.txt", e.Data + Environment.NewLine);

            if (e.Data is not null && e.Data.Contains("Application startup complete."))
                runCompleted.TrySetResult(true);

            if (e.Data is not null && e.Data.Contains("error", StringComparison.OrdinalIgnoreCase))
                runCompleted.TrySetResult(false);
        };

        _runProcess_python.Start();
        _runProcess_python.BeginOutputReadLine();
        _runProcess_python.BeginErrorReadLine();

        if (!await WaitForStartupAsync(runCompleted.Task))
            throw new Exception("Unable to launch Nexus.Agent (python).");
    }

    private static async Task<bool> WaitForStartupAsync(Task<bool> task)
    {
        try
        {
            return await task.WaitAsync(TimeSpan.FromMinutes(1));
        }
        catch (TimeoutException)
        {
            return false;
        }
    }

    public Task DisposeAsync()
    {
        _buildProcess_dotnet?.Kill();
        _runProcess_dotnet?.Kill();
        _runProcess_python?.Kill();

        return Task.CompletedTask;
    }
}
