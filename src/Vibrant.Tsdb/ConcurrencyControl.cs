using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class ConcurrencyControl : IConcurrencyControl, IDisposable
   {
      private bool _disposed = false; 
      private SemaphoreSlim _read;
      private SemaphoreSlim _write;

      public ConcurrencyControl(int read, int write )
      {
         _read = new SemaphoreSlim( read );
         _write = new SemaphoreSlim( write );
      }

      public async Task<IDisposable> ReadAsync()
      {
         var op = new Operation( _read );
         await op.WaitAsync().ConfigureAwait( false );
         return op;
      }

      public async Task<IDisposable> WriteAsync()
      {
         var op = new Operation( _write );
         await op.WaitAsync().ConfigureAwait( false );
         return op;
      }

      class Operation : IDisposable
      {
         private bool _released = false;
         private SemaphoreSlim _sem;

         public Operation( SemaphoreSlim sem )
         {
            _sem = sem;
         }

         public Task WaitAsync()
         {
            return _sem.WaitAsync();
         }

         #region IDisposable Support

         public void Dispose()
         {
            if( !_released )
            {
               _sem.Release();
               _released = true;
            }
         }

         #endregion
      }

      #region IDisposable Support

      protected virtual void Dispose( bool disposing )
      {
         if( !_disposed )
         {
            if( disposing )
            {
               _read.Dispose();
               _write.Dispose();
            }

            _disposed = true;
         }
      }
      
      public void Dispose()
      {
         Dispose( true );
      }

      #endregion
   }
}
