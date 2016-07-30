using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.ConsoleApp.Entries
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

      public byte GetTypeCode()
      {
         return 1;
      }

      public void Read( BinaryReader reader )
      {
         Timestamp = new DateTime( reader.ReadInt64(), DateTimeKind.Utc );
         Value = reader.ReadDouble();
      }

      public void Write( BinaryWriter writer )
      {
         writer.Write( Timestamp.Ticks );
         writer.Write( Value );
      }
   }
}
