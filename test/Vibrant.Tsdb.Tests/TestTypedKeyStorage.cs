using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Tests.Entries;
using Vibrant.Tsdb.Tests.Model;

namespace Vibrant.Tsdb.Tests
{
   public class TestTypedKeyStorage : ITypedKeyStorage<string, MeasureType>
   {
      private Dictionary<string, MeasureType> _measureTypes;
      private string[] _keys;

      public TestTypedKeyStorage( IEnumerable<string> availableKeys )
      {
         _measureTypes = new Dictionary<string, MeasureType>();
         _measureTypes.Add( "Temperature", new MeasureType( "Temperature" ) );
         _keys = availableKeys.ToArray();
      }

      public Task<MeasureType> GetMeasureTypeAsync( string measureTypeName )
      {
         return Task.FromResult( _measureTypes[ measureTypeName ] );
      }

      public Task<IEnumerable<ITypedKey<string, MeasureType>>> GetTaggedKeysAsync( IEnumerable<string> keys )
      {
         var result = new List<ITypedKey<string, MeasureType>>();
         var measureType = _measureTypes[ "Temperature" ];
         foreach( var key in keys )
         {
            result.Add( new TypedKey( key, measureType ) );
         }
         return Task.FromResult<IEnumerable<ITypedKey<string, MeasureType>>>( result );
      }

      public Task<IEnumerable<ITypedKey<string, MeasureType>>> GetTaggedKeysAsync( string measureTypeName, IEnumerable<KeyValuePair<string, string>> requiredTags )
      {
         var result = new List<ITypedKey<string, MeasureType>>();
         var measureType = _measureTypes[ measureTypeName ];
         foreach( var key in _keys )
         {
            result.Add( new TypedKey( key, measureType ) );
         }
         return Task.FromResult<IEnumerable<ITypedKey<string, MeasureType>>>( result );
      }
   }
}
