using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats
{
   public class AtsOperation
   {
      public AtsOperation( TsdbTableEntity row, AtsOperationType operationType )
      {
         Row = row;
         OperationType = operationType;
      }

      public TsdbTableEntity Row { get; private set; }

      public AtsOperationType OperationType { get; private set; }
   }
}
