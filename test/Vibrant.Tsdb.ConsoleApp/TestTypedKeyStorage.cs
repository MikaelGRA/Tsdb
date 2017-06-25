using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.ConsoleApp.Entries;
using Vibrant.Tsdb.ConsoleApp.Model;

namespace Vibrant.Tsdb.ConsoleApp
{
   public class TestTypedKeyStorage : ITypedKeyStorage<BasicKey, MeasureType>
   {
      private Dictionary<string, MeasureType> _measureTypes;
      private BasicKey[] _keys;

      public TestTypedKeyStorage( IEnumerable<BasicKey> availableKeys )
      {
         _measureTypes = new Dictionary<string, MeasureType>();
         _measureTypes.Add( "Temperature", new MeasureType( "Temperature" ) );
         _keys = availableKeys.ToArray();
      }

      public Task<MeasureType> GetMeasureTypeAsync( string measureTypeName )
      {
         return Task.FromResult( _measureTypes[ measureTypeName ] );
      }

      public Task<IEnumerable<ITypedKey<BasicKey, MeasureType>>> GetTaggedKeysAsync( IEnumerable<BasicKey> keys )
      {
         var result = new List<ITypedKey<BasicKey, MeasureType>>();
         var measureType = _measureTypes[ "Temperature" ];
         foreach( var key in keys )
         {
            result.Add( new TypedKey( key, measureType ) );
         }
         return Task.FromResult<IEnumerable<ITypedKey<BasicKey, MeasureType>>>( result );
      }

      public Task<IEnumerable<ITypedKey<BasicKey, MeasureType>>> GetTaggedKeysAsync( string measureTypeName, IEnumerable<KeyValuePair<string, string>> requiredTags )
      {
         var result = new List<ITypedKey<BasicKey, MeasureType>>();
         var measureType = _measureTypes[ measureTypeName ];
         foreach( var key in _keys )
         {
            result.Add( new TypedKey( key, measureType ) );
         }
         return Task.FromResult<IEnumerable<ITypedKey<BasicKey, MeasureType>>>( result );
      }
   }
}
