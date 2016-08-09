using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Ats;
using Vibrant.Tsdb.Files;
using Vibrant.Tsdb.Redis;
using Vibrant.Tsdb.Sql;

namespace Vibrant.Tsdb.ConsoleApp.Entries
{
   public class BasicEntry : IEntry<string>, IAtsEntry<string>, ISqlEntry<string>, IRedisEntry<string>, IFileEntry<string>
   {
      private KeyValuePair<string, string>[] _empty = new KeyValuePair<string, string>[ 0 ];

      public string Id { get; set; }

      public DateTime Timestamp { get; set; }

      public double Value { get; set; }

      public string GetKey()
      {
         return Id;
      }

      public void SetKey( string id )
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

      public ushort GetTypeCode()
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
