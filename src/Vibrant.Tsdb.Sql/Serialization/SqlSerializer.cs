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

      public static void SerializeEntry<TKey, TEntry>( BinaryWriter writer, TEntry entry )
         where TEntry : ISqlEntry<TKey>
      {
         entry.Write( writer );
      }

      public static TEntry DeserializeEntry<TKey, TEntry>( BinaryReader reader, TKey id )
         where TEntry : ISqlEntry<TKey>, new()
      {
         var entry = new TEntry();
         entry.SetKey( id );
         entry.Read( reader );
         return entry;
      }

      public static void Serialize<TKey, TEntry>( IEnumerable<TEntry> entries, Action<TEntry, byte[]> serialized )
         where TEntry : ISqlEntry<TKey>
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

      public static List<TEntry> Deserialize<TKey, TEntry>( IEnumerable<SqlEntry> sqlEntries, IKeyConverter<TKey> keyConverter )
         where TEntry : ISqlEntry<TKey>, new()
      {
         List<TEntry> entries = new List<TEntry>();

         foreach( var sqlEntry in sqlEntries )
         {
            var stream = new MemoryStream( sqlEntry.Data );
            var reader = CreateReader( stream );
            var entry = DeserializeEntry<TKey, TEntry>( reader, keyConverter.Convert( sqlEntry.Id ) );
            entry.SetTimestamp( sqlEntry.Timestamp );
            entries.Add( entry );
         }

         return entries;
      }
   }
}
