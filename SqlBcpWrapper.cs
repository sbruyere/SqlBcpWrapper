using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Pipes;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Silii.Utils.SqlBcpWrapper
{
    public class SqlBcpWrapper
    {
        private Process _sqlBcpProcess;

        readonly string _serverName;
        readonly string _username;
        readonly string _password;

        private const string BCP_CMD = "\"{0}\" queryout \"{1}\" -c -t, -S \"{2}\" -U {3} -P \"{4}\"";

        public event EventHandler<DataReceivedEventArgs> ErrorReceived;

        public event EventHandler<DataReceivedEventArgs> OutputReceived;

        public SqlBcpWrapper(string serverName, string username, string password)
        {
            _serverName = serverName;
            _username = username;
            _password = password;

            ToolPath = ResolveAppBinPath();
            SqlBcpExeName = "bcp";
            ProcessPriority = ProcessPriorityClass.Normal;
        }

        public void Abort()
        {
            EnsureProcessStopped();
        }

        private static void CheckExitCode(int exitCode, List<string> errLines)
        {
            if (exitCode != 0)
            {
                throw new SqlBcpException(exitCode, string.Join("\n", errLines.ToArray()));
            }
        }

        protected void CopyToStdIn(Stream inputStream)
        {
            byte[] buffer = new byte[0x2000];
            while (true)
            {
                int count = inputStream.Read(buffer, 0, buffer.Length);
                if (count <= 0)
                {
                    break;
                }
                _sqlBcpProcess.StandardInput.BaseStream.Write(buffer, 0, count);
                _sqlBcpProcess.StandardInput.BaseStream.Flush();
            }
            _sqlBcpProcess.StandardInput.Close();
        }

        private static void DeleteFileIfExists(string filePath)
        {
            if ((filePath != null) && File.Exists(filePath))
            {
                try
                {
                    File.Delete(filePath);
                }
                catch
                {
                    // ignored
                }
            }
        }

        private void EnsureProcessStopped()
        {
            if ((_sqlBcpProcess != null) && !_sqlBcpProcess.HasExited)
            {
                try
                {
                    _sqlBcpProcess.Kill();
                    _sqlBcpProcess = null;
                }
                catch (Exception)
                {
                    // ignored
                }
            }
        }

        private static string PrepareCmdArg(string arg)
        {
            StringBuilder builder = new StringBuilder();
            builder.Append('"');
            builder.Append(arg.Replace("\"", "\\\""));
            builder.Append('"');
            return builder.ToString();
        }

        private static void ReadStdOutToStream(Process proc, Stream outputStream)
        {
            int num;
            byte[] buffer = new byte[0x8000];
            while ((num = proc.StandardOutput.BaseStream.Read(buffer, 0, buffer.Length)) > 0)
            {
                outputStream.Write(buffer, 0, num);
            }
        }

        private static string ResolveAppBinPath()
        {
            string baseDirectory = AppDomain.CurrentDomain.BaseDirectory;
            foreach (Assembly assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                Type type = assembly.GetType("System.Web.HttpRuntime", false);
                if (type != null)
                {
                    PropertyInfo property = type.GetProperty("AppDomainId", BindingFlags.Public | BindingFlags.Static);
                    if ((property != null) && (property.GetValue(null, null) != null))
                    {
                        PropertyInfo info2 = type.GetProperty("BinDirectory", BindingFlags.Public | BindingFlags.Static);
                        if (info2 != null)
                        {
                            object obj2 = info2.GetValue(null, null);
                            var directory = obj2 as string;
                            if (directory != null)
                            {
                                baseDirectory = directory;
                            }
                        }
                        return baseDirectory;
                    }
                }
            }
            return baseDirectory;
        }


        public Stream Run(Stream output, string query, params string[] sqlArguments)
        {
            RunAsync(output, query, sqlArguments).Wait();

            return output;
        }

        public async Task<Stream> RunAsync(Stream output, string query, params string[] sqlArguments)
        {
            NamedPipeServerStream pipeServerStream = new NamedPipeServerStream("SqlBcpWrapper" + GetHashCode(), PipeDirection.InOut, 1, PipeTransmissionMode.Message, PipeOptions.Asynchronous);

            pipeServerStream.BeginWaitForConnection(e =>
            {
                pipeServerStream.EndWaitForConnection(e);

                while (pipeServerStream.IsConnected && !pipeServerStream.IsMessageComplete)
                {
                }

                pipeServerStream.CopyTo(output);
                output.Seek(0, SeekOrigin.Begin);
                pipeServerStream.Disconnect();
                pipeServerStream.Close();
            }, this);
            
            RunInternal("\\\\.\\pipe\\SqlBcpWrapper" + GetHashCode(), query, sqlArguments, null, null);

            await Task.Run(() =>
            {
                while (pipeServerStream.IsConnected && !pipeServerStream.IsMessageComplete)
                {
                }
            });

            return output;
        }

        public void Run(string destination, string query, params string[] sqlArguments)
        {
            RunInternal(destination, query, sqlArguments, null, null);
        }

        public void Run(string destination, string query, Stream outputStream)
        {
            RunInternal(destination, query, null, null, outputStream);
        }

        public void Run(string destination, string query, Stream inputStream, Stream outputStream)
        {
            RunInternal(destination, query, null, inputStream, outputStream);
        }

        public void Run(string destination, string query, Stream outputStream = null, params string[] sqlArgs)
        {
            RunInternal(destination, query, sqlArgs, null, outputStream);
        }

        public void Run(string destination, string query, Stream inputStream = null, Stream outputStream = null, params string[] sqlArgs)
        {
            RunInternal(destination, query, sqlArgs, inputStream, outputStream);
        }

        private void RunInternal(string destination, string query, string[] sqlArgs, Stream inputStream, Stream outputStream)
        {
            if (query == null)
            {
                throw new ArgumentNullException("query");
            }

            try
            {
                var qQuery = string.Format(query, sqlArgs.Cast<object>().ToArray());
                var bcpCommand = BuildBcpCommand(_serverName, _username, _password, destination, qQuery);
                
                string directoryName;
                if (ToolPath != null && (directoryName = Path.GetDirectoryName(ToolPath)) != null)
                {
                    ProcessStartInfo startInfo = new ProcessStartInfo(SqlBcpExeName, bcpCommand)
                    {
                        WindowStyle = ProcessWindowStyle.Hidden,
                        CreateNoWindow = true,
                        UseShellExecute = false,
                        WorkingDirectory = directoryName,
                        RedirectStandardInput = inputStream != null,
                        RedirectStandardOutput = true,
                        RedirectStandardError = true
                    };
                    
                    _sqlBcpProcess = Process.Start(startInfo);
                }

                if (_sqlBcpProcess != null)
                {
                    if (ProcessPriority != ProcessPriorityClass.Normal)
                    {
                        _sqlBcpProcess.PriorityClass = ProcessPriority;
                    }
                    List<string> errorLines = new List<string>();

                    _sqlBcpProcess.ErrorDataReceived += delegate (object o, DataReceivedEventArgs args)
                    {
                        if (args.Data != null)
                        {
                            errorLines.Add(args.Data);
                            if (this.ErrorReceived != null)
                            {
                                this.ErrorReceived(this, args);
                            }
                        }
                    };
                    _sqlBcpProcess.BeginErrorReadLine();
                    if (outputStream == null)
                    {
                        DataReceivedEventHandler handler = delegate (object o, DataReceivedEventArgs args)
                        {
                            if (OutputReceived != null)
                            {
                                this.OutputReceived(this, args);
                            }
                        };
                        _sqlBcpProcess.OutputDataReceived += handler;
                        _sqlBcpProcess.BeginOutputReadLine();
                    }
                    if (inputStream != null)
                    {
                        CopyToStdIn(inputStream);
                    }
                    if (outputStream != null)
                    {
                        ReadStdOutToStream(_sqlBcpProcess, outputStream);
                    }
                    WaitProcessForExit();
                    CheckExitCode(_sqlBcpProcess.ExitCode, errorLines);
                    _sqlBcpProcess.Close();
                }
                _sqlBcpProcess = null;
            }
            catch (Exception exception)
            {
                throw new Exception("Cannot execute BCP command: " + exception.Message, exception);
            }
        }

        //public string BuildSqlQuery(string dbName, DateTime startDate, DateTime endDate)
        //{
        //    return string.Format(SQL_RQST, dbName, startDate, endDate);
        //}

        //private string BuildQuery(string query, string[] sqlArguments)
        //{
        //    StringBuilder builder = new StringBuilder();
        //    if (!string.IsNullOrEmpty(CustomArgs))
        //    {
        //        builder.AppendFormat(" {0} ", CustomArgs);
        //    }
        //    builder.AppendFormat(" {0}", PrepareCmdArg(query));
        //    if (sqlArguments != null)
        //    {
        //        foreach (string str2 in sqlArguments)
        //        {
        //            builder.AppendFormat(" {0}", PrepareCmdArg(str2));
        //        }
        //    }
        //    return builder.ToString();
        //}


        private void WaitProcessForExit()
        {
            bool hasValue = ExecutionTimeout.HasValue;
            if (hasValue)
            {
                _sqlBcpProcess.WaitForExit((int)ExecutionTimeout.Value.TotalMilliseconds);
            }
            else
            {
                _sqlBcpProcess.WaitForExit();
            }
            if (_sqlBcpProcess == null)
            {
                throw new SqlBcpException(-1, "Bcp process was aborted");
            }
            if (hasValue && !_sqlBcpProcess.HasExited)
            {
                EnsureProcessStopped();
                throw new SqlBcpException(-2,
                    string.Format("Bcp process exceeded execution timeout ({0}) and was aborted",
                        ExecutionTimeout));
            }
        }

        private string BuildBcpCommand(string serverName, string username, string password, string destination, string query)
        {
            return string.Format(BCP_CMD, query, destination, serverName, username, password);
        }

        public string CustomArgs { get; set; }

        public TimeSpan? ExecutionTimeout { get; set; }

        public string SqlBcpExeName { get; set; }

        public ProcessPriorityClass ProcessPriority { get; set; }

        public string ToolPath { get; set; }
    }
}
