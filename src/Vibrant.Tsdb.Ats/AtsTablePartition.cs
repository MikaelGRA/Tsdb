using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats
{
   public struct AtsTablePartition : IEquatable<AtsTablePartition>
   {
      public AtsTablePartition( ITable table, string partitionKey )
      {
         Table = table;
         PartitionKey = partitionKey;
      }

      public ITable Table { get; private set; }

      public string PartitionKey { get; private set; }

      public bool Equals( AtsTablePartition other )
      {
         return Table.Equals( other.Table ) && PartitionKey == other.PartitionKey;
      }

      // override object.Equals
      public override bool Equals( object obj )
      {
         return obj is AtsTablePartition && Equals( (AtsTablePartition)obj );
      }

      // override object.GetHashCode
      public override int GetHashCode()
      {
         return Table.GetHashCode() + PartitionKey.GetHashCode();
      }
   }
}
