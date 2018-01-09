using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.CosmosTables
{
   public struct CosmosTablesTablePartition : IEquatable<CosmosTablesTablePartition>
   {
      public CosmosTablesTablePartition( ITable table, string partitionKey )
      {
         Table = table;
         PartitionKey = partitionKey;
      }

      public ITable Table { get; private set; }

      public string PartitionKey { get; private set; }

      public bool Equals( CosmosTablesTablePartition other )
      {
         return Table.Equals( other.Table ) && PartitionKey == other.PartitionKey;
      }

      // override object.Equals
      public override bool Equals( object obj )
      {
         return obj is CosmosTablesTablePartition && Equals( (CosmosTablesTablePartition)obj );
      }

      // override object.GetHashCode
      public override int GetHashCode()
      {
         return Table.GetHashCode() + PartitionKey.GetHashCode();
      }
   }
}
