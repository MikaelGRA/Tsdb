using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Vibrant.Tsdb;
using Vibrant.Tsdb.Exceptions;

namespace Vibrant.Tsdb.Files
{
   public class TemporaryFileStorage<TKey, TEntry> : ITemporaryStorage<TKey, TEntry>
      where TEntry : IFileEntry, new()
   {
      public const int DefaultMaxFileSize = 1 * 1024 * 1024;
      public const long DefaultMaxStorageSize = 10L * 1024L * 1024L * 1024L;
      private static readonly string FileTemplate = "{0}.dat";
      private static readonly Task _completed = Task.FromResult( true );
      
      private SemaphoreSlim _sem;
      private DirectoryInfo _directory;
      private FileInfo _currentFile;
      private int _maxFileSize;
      private long _maxStorageSize;
      private long _currentSize;
      private IKeyConverter<TKey> _keyConverter;

      public TemporaryFileStorage( string directory, int maxFileSize, long maxStorageSize, IKeyConverter<TKey> keyConverter )
      {
         _directory = new DirectoryInfo( directory );
         _maxFileSize = maxFileSize;
         _maxStorageSize = maxStorageSize;
         _keyConverter = keyConverter;
         _sem = new SemaphoreSlim( 1, 1 );

         CalculateDirectorySize();
      }

      public TemporaryFileStorage( string directory, int maxFileSize, long maxStorageSize )
         : this( directory, maxFileSize, maxStorageSize, DefaultKeyConverter<TKey>.Current )
      {
      }

      public TemporaryFileStorage( string directory )
         : this( directory, DefaultMaxFileSize, DefaultMaxStorageSize, DefaultKeyConverter<TKey>.Current )
      {
      }

      public async Task<TemporaryReadResult<TKey, TEntry>> ReadAsync( int count )
      {
         await _sem.WaitAsync().ConfigureAwait( false );
         try
         {
            int read = 0;
            Dictionary<string, Serie<TKey, TEntry>> series = new Dictionary<string, Serie<TKey, TEntry>>();
            List<FileModificationRef> modifications = new List<FileModificationRef>();
            foreach( var fi in _directory.EnumerateFiles( "*.dat" ) )
            {
               using( var fs = fi.Open( FileMode.Open, FileAccess.Read ) )
               using( var reader = new BinaryReader( fs, Encoding.ASCII, true ) )
               {

                  while( reader.PeekChar() != -1 && read < count )
                  {
                     var entry = new TEntry();
                     var id = reader.ReadString();
                     Serie<TKey, TEntry> serie;
                     if( !series.TryGetValue( id, out serie ) )
                     {
                        var key = await _keyConverter.ConvertAsync( id ).ConfigureAwait( false );
                        serie = new Serie<TKey, TEntry>( key );
                        series.Add( id, serie );
                     }

                     var timestamp = new DateTime( reader.ReadInt64(), DateTimeKind.Utc );
                     entry.SetTimestamp( timestamp );
                     entry.Read( reader );
                     serie.Entries.Add( entry );
                     read++;
                  }

                  // add the file
                  var reference = new FileModificationRef( fi, fs.Length, fs.Position );
                  modifications.Add( reference );

                  if( read == count )
                  {
                     break;
                  }
               }
            }

            return new TemporaryReadResult<TKey, TEntry>( series.Values.ToList(), () => DeleteAsync( modifications ) );
         }
         finally
         {
            _sem.Release();
         }
      }

      public Task WriteAsync( IEnumerable<ISerie<TKey, TEntry>> series )
      {
         _sem.Wait();
         try
         {
            if( _currentSize > _maxStorageSize )
            {
               throw new TsdbException( "Could not write any of the entries to the temporary file storage because it exceeds the maximum storage size." );
            }

            // Open the current or a new file
            FileInfo fileInfo = GetOrCreateCurrentFile();
            FileStream fileStream = fileInfo.Open( FileMode.OpenOrCreate, FileAccess.ReadWrite );
            long startFileSize = fileStream.Length;

            // Seek to the end of the file
            fileStream.Seek( 0, SeekOrigin.End );

            // Create the writer
            BinaryWriter writer = new BinaryWriter( fileStream, Encoding.ASCII, true );

            bool disposed = false;

            long beforeLength = startFileSize;
            long afterLength = beforeLength;
            foreach( var serie in series )
            {
               var key = serie.GetKey();
               var id = _keyConverter.Convert( key );

               foreach( var entry in serie.GetEntries() )
               {
                  beforeLength = afterLength;

                  if( beforeLength > _maxFileSize )
                  {
                     // flush to file
                     writer.Flush();

                     // increment storage size
                     _currentSize += ( beforeLength - startFileSize );

                     // close the current file stream
                     writer.Dispose();
                     fileStream.Dispose();

                     // create a new file reference...
                     fileInfo = CreateCurrentFile();
                     fileStream = fileInfo.Open( FileMode.OpenOrCreate, FileAccess.ReadWrite );
                     writer = new BinaryWriter( fileStream, Encoding.ASCII, true );
                     startFileSize = fileStream.Length;
                     beforeLength = startFileSize;
                     afterLength = beforeLength;
                  }

                  long storageSize = _currentSize + ( beforeLength - startFileSize );
                  if( storageSize > _maxStorageSize )
                  {
                     // flush to file
                     writer.Flush();

                     // increment storage size
                     _currentSize += ( beforeLength - startFileSize );

                     // close the current file stream
                     writer.Dispose();
                     fileStream.Dispose();
                     disposed = true;

                     // throw exception indicating which entries could not be inserted (with skip)
                     throw new TsdbException( "Could not write all the entries to the temporary file storage because it exceeds the maximum storage size." );
                  }

                  writer.Write( id );
                  writer.Write( entry.GetTimestamp().Ticks );
                  entry.Write( writer );

                  afterLength = fileStream.Length;
               }
            }

            writer.Flush();
            writer.Dispose();
            fileStream.Dispose();
         }
         finally
         {
            _sem.Release();
         }

         return _completed;
      }

      public Task DeleteAsync()
      {
         _sem.Wait();
         try
         {
            foreach( var fi in _directory.EnumerateFiles( "*.dat" ) )
            {
               _currentSize -= fi.Length;
               fi.Delete();
            }
         }
         finally
         {
            _sem.Release();
         }
         return _completed;
      }

      private Task DeleteAsync( List<FileModificationRef> modifications )
      {
         _sem.Wait();
         try
         {
            foreach( var mod in modifications )
            {
               _currentSize -= mod.DeleteLength;
               mod.PerformModification();
            }
         }
         finally
         {
            _sem.Release();
         }
         return _completed;
      }

      private void CalculateDirectorySize()
      {
         if( !_directory.Exists )
         {
            _directory.Create();
         }

         _currentSize = 0;
         foreach( var fi in _directory.EnumerateFiles( "*.dat" ) )
         {
            _currentSize += fi.Length;
         }
      }

      private FileInfo GetOrCreateCurrentFile()
      {
         if( _currentFile != null )
         {
            if( !_currentFile.Exists )
            {
               _currentFile = CreateCurrentFile();
            }

            return _currentFile;
         }

         return CreateCurrentFile();
      }

      private FileInfo CreateCurrentFile()
      {
         var timestamp = DateTime.UtcNow;
         var fi = new FileInfo( Path.Combine( _directory.FullName, CreateFileName( timestamp ) ) );
         while( fi.Exists )
         {
            timestamp += TimeSpan.FromMilliseconds( 10 );
            fi = new FileInfo( Path.Combine( _directory.FullName, CreateFileName( timestamp ) ) );
         }

         _currentFile = fi;

         return fi;
      }

      private string CreateFileName( DateTime timestamp )
      {
         return string.Format( FileTemplate, timestamp.ToString( "yyyyMMddHHmmssffff" ) );
      }
   }
}
