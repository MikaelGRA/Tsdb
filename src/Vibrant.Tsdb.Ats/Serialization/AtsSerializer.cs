using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats.Serialization
{
   internal static class AtsSerializer
   {
      private static readonly int MaxEntrySizeInBytes = 1024;

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
         where TEntry : IAtsEntry<TKey>
      {
         writer.Write( entry.GetTimestamp().Ticks );
         entry.Write( writer );
      }

      public static TEntry DeserializeEntry<TKey, TEntry>( BinaryReader reader, TKey id )
         where TEntry : IAtsEntry<TKey>, new()
      {
         var entry = new TEntry();
         entry.SetKey( id );
         entry.SetTimestamp( new DateTime( reader.ReadInt64(), DateTimeKind.Utc ) );
         entry.Read( reader );
         return entry;
      }

      public static List<AtsSerializationResult> Serialize<TKey, TEntry>( List<TEntry> entries, int maxByteArraySize )
         where TEntry : IAtsEntry<TKey>
      {
         var results = new List<AtsSerializationResult>();
         var stream = new MemoryStream();
         var writer = CreateWriter( stream );
         DateTime? from = null;

         int currentSize = 0;
         List<byte[]> serializedEntries = new List<byte[]>();

         for( int i = entries.Count - 1 ; i > -1 ; i-- )
         {
            var entry = entries[ i ];

            var timestamp = entry.GetTimestamp();
            if( !from.HasValue )
            {
               from = timestamp;
            }

            if( currentSize + MaxEntrySizeInBytes > maxByteArraySize )
            {
               writer.Write( serializedEntries.Count );
               var length = stream.ToArray();

               // reset stream
               stream.Seek( 0, SeekOrigin.Begin );
               stream.SetLength( 0 );

               // create big array from mall the small arrays
               var data = CreateData( currentSize, length, serializedEntries );

               // add the created result
               results.Add( new AtsSerializationResult( from.Value, data ) );

               // reset parameters
               from = null;
               currentSize = 0;
               serializedEntries = new List<byte[]>( serializedEntries.Count );
            }

            SerializeEntry<TKey, TEntry>( writer, entry );
            writer.Flush(); // is this needed for a memory stream????

            var serializedEntry = stream.ToArray();
            serializedEntries.Add( serializedEntry );

            // update parameters
            currentSize += serializedEntry.Length;

            // reset stream
            stream.Seek( 0, SeekOrigin.Begin );
            stream.SetLength( 0 );
         }

         if( currentSize > 0 )
         {
            writer.Write( serializedEntries.Count );
            var length = stream.ToArray();

            // reset stream
            stream.Seek( 0, SeekOrigin.Begin );
            stream.SetLength( 0 );

            var data = CreateData( currentSize, length, serializedEntries );

            results.Add( new AtsSerializationResult( from.Value, data ) );
         }

         writer.Dispose();

         return results;
      }

      private static byte[] CreateData( int size, byte[] length, List<byte[]> serializedEntries )
      {
         var data = new byte[ size + length.Length ];
         Buffer.BlockCopy( length, 0, data, 0, length.Length );

         var copied = length.Length;
         for( int j = serializedEntries.Count - 1 ; j > -1 ; j-- )
         {
            var serializedEntry = serializedEntries[ j ];
            Buffer.BlockCopy( serializedEntry, 0, data, copied, serializedEntry.Length );
            copied += serializedEntry.Length;
         }
         return data;
      }

      public static TEntry[] Deserialize<TKey, TEntry>( TKey id, byte[] bytes, Sort sort )
         where TEntry : IAtsEntry<TKey>, new()
      {
         var stream = new MemoryStream( bytes );
         var reader = CreateReader( stream );
         var count = reader.ReadInt32();
         TEntry[] entries = new TEntry[ count ];

         if( sort == Sort.Ascending )
         {
            int idx = count;
            while( stream.Length != stream.Position )
            {
               var entry = DeserializeEntry<TKey, TEntry>( reader, id );
               entries[ --idx ] = entry;
            }
         }
         else
         {
            int idx = 0;
            while( stream.Length != stream.Position )
            {
               var entry = DeserializeEntry<TKey, TEntry>( reader, id );
               entries[ idx++ ] = entry;
            }
         }

         return entries;
      }
   }
}
