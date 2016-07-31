using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats.Tests.Entries
{
   public class BasicEntry : IEntry
   {
      public string Id { get; set; }

      public DateTime Timestamp { get; set; }

      public double Value { get; set; }

      public string GetId()
      {
         return Id;
      }

      public void SetId( string id )
      {
         Id = id;
      }

      public DateTime GetTimestamp()
      {
         return Timestamp;
      }

      public void SetTimestamp( DateTime timestamp )
      {
         Timestamp = timestamp;
      }

      public byte GetTypeCode()
      {
         return 1;
      }

      public void Read( BinaryReader reader )
      {
         Value = reader.ReadDouble();
      }

      public void Write( BinaryWriter writer )
      {
         writer.Write( Value );
      }
   }
}
