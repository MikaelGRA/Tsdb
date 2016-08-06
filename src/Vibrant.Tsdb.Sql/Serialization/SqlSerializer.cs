using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Sql.Serialization
{
   internal static class SqlSerializer
   {
      public static BinaryReader CreateReader( Stream stream )
      {
         var reader = new BinaryReader( stream, Encoding.ASCII );
         return reader;
      }

      public static BinaryWriter CreateWriter( Stream stream )
      {
         var writer = new BinaryWriter( stream, Encoding.ASCII );
         return writer;
      }

      public static void SerializeEntry<TEntry>( BinaryWriter writer, TEntry entry )
         where TEntry : ISqlEntry
      {
         entry.Write( writer );
      }

      public static TEntry DeserializeEntry<TEntry>( BinaryReader reader, string id )
         where TEntry : ISqlEntry, new()
      {
         var entry = new TEntry();
         entry.SetId( id );
         entry.Read( reader );
         return entry;
      }

      public static void Serialize<TEntry>( IEnumerable<TEntry> entries, Action<TEntry, byte[]> serialized )
         where TEntry : ISqlEntry
      {
         var stream = new MemoryStream();
         var writer = CreateWriter( stream );

         foreach( var entry in entries )
         {
            SerializeEntry( writer, entry );
            writer.Flush();

            var serializedEntry = stream.ToArray();

            serialized( entry, serializedEntry );

            // reset stream
            stream.Seek( 0, SeekOrigin.Begin );
            stream.SetLength( 0 );
         }

         writer.Dispose();
      }

      public static List<TEntry> Deserialize<TEntry>( IEnumerable<SqlEntry> sqlEntries )
         where TEntry : ISqlEntry, new()
      {
         List<TEntry> entries = new List<TEntry>();

         foreach( var sqlEntry in sqlEntries )
         {
            var stream = new MemoryStream( sqlEntry.Data );
            var reader = CreateReader( stream );
            var entry = DeserializeEntry<TEntry>( reader, sqlEntry.Id );
            entry.SetTimestamp( sqlEntry.Timestamp );
            entries.Add( entry );
         }

         return entries;
      }
   }
}
