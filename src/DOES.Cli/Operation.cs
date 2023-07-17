using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DOES.Cli
{
    public abstract class Operation
    {
        public abstract bool VerboseWriter
        {
            get;
            set;
        }

        public abstract CancellationTokenSource TokenSource
        {
            get;
        }

        public abstract void ExecuteOperation();
    }


}
