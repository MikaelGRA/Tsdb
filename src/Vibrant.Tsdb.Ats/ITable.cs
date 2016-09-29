using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats
{
   public interface ITable : IEquatable<ITable>
   {
      string Suffix { get; }

      DateTime From { get; }
      
      DateTime To { get; }
   }
}
