using System;

namespace Silii.Utils.SqlBcpWrapper
{
    public class SqlBcpException : Exception
    {
        public SqlBcpException(int errCode, string message)
            : base(message)
        {
            ErrorCode = errCode;
        }

        public int ErrorCode { get; private set; }
    }
}
