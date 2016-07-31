using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Serialization;

namespace Vibrant.Tsdb.Sql.Serialization
{
   internal static class SqlSerializer
   {
      public static void Serialize( IEnumerable<IEntry> entries, Action<IEntry, byte[]> serialized )
      {
         var stream = new MemoryStream();
         var writer = EntrySerializer.CreateWriter( stream );

         foreach( var entry in entries )
         {
            EntrySerializer.SerializeEntry( writer, entry, false, false );
            writer.Flush();

            var serializedEntry = stream.ToArray();

            serialized( entry, serializedEntry );

            // reset stream
            stream.Seek( 0, SeekOrigin.Begin );
            stream.SetLength( 0 );
         }

         writer.Dispose();
      }

      public static List<IEntry> Deserialize( IEnumerable<SqlEntry> sqlEntries )
      {
         List<IEntry> entries = new List<IEntry>();

         foreach( var sqlEntry in sqlEntries )
         {
            var stream = new MemoryStream( sqlEntry.Data );
            var reader = EntrySerializer.CreateReader( stream );
            var entry = EntrySerializer.DeserializeEntry( reader, sqlEntry.Id, false );
            entry.SetTimestamp( sqlEntry.Timestamp );
            entries.Add( entry );
         }

         return entries;
      }
   }
}
