using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.CosmosTables
{
   internal class CosmosTablesOperation
   {
      public CosmosTablesOperation( TsdbTableEntity row, CosmosTablesOperationType operationType )
      {
         Row = row;
         OperationType = operationType;
      }

      public TsdbTableEntity Row { get; private set; }

      public CosmosTablesOperationType OperationType { get; private set; }
   }
}
