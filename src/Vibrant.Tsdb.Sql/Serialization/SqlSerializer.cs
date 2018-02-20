using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vibrant.Tsdb.Helpers;

namespace Vibrant.Tsdb.Sql.Serialization
{
   internal static class SqlSerializer
   {
      public static BinaryReader CreateReader( Stream stream )
      {
         var reader = new BinaryReader( stream, TsdbEncodings.Default );
         return reader;
      }

      public static BinaryWriter CreateWriter( Stream stream )
      {
         var writer = new BinaryWriter( stream, TsdbEncodings.Default );
         return writer;
      }

      public static void SerializeEntry<TKey, TEntry>( BinaryWriter writer, TEntry entry )
         where TEntry : ISqlEntry
      {
         entry.Write( writer );
      }

      public static TEntry DeserializeEntry<TKey, TEntry>( BinaryReader reader )
         where TEntry : ISqlEntry, new()
      {
         var entry = new TEntry();
         entry.Read( reader );
         return entry;
      }

      public static void Serialize<TKey, TEntry>( IEnumerable<TEntry> entries, Action<TEntry, byte[]> serialized )
         where TEntry : ISqlEntry
      {
         var stream = new MemoryStream();
         var writer = CreateWriter( stream );

         foreach( var entry in entries )
         {
            SerializeEntry<TKey, TEntry>( writer, entry );
            writer.Flush();

            var serializedEntry = stream.ToArray();

            serialized( entry, serializedEntry );

            // reset stream
            stream.Seek( 0, SeekOrigin.Begin );
            stream.SetLength( 0 );
         }

         writer.Dispose();
      }

      public static TEntry Deserialize<TKey, TEntry>( SqlEntry sqlEntry )
         where TEntry : ISqlEntry, new()
      {
         var stream = new MemoryStream( sqlEntry.Data );
         var reader = CreateReader( stream );
         var entry = DeserializeEntry<TKey, TEntry>( reader );
         entry.SetTimestamp( sqlEntry.Timestamp );
         return entry;
      }

      public static List<TEntry> Deserialize<TKey, TEntry>( IEnumerable<SqlEntry> sqlEntries )
         where TEntry : ISqlEntry, new()
      {
         List<TEntry> entries = new List<TEntry>();
         foreach( var sqlEntry in sqlEntries )
         {
            entries.Add( Deserialize<TKey, TEntry>( sqlEntry ) );
         }
         return entries;
      }
   }
}
