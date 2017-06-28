using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vibrant.InfluxDB.Client;
using Vibrant.InfluxDB.Client.Rows;
using Vibrant.Tsdb.Exceptions;

namespace Vibrant.Tsdb.InfluxDB
{
   public class InfluxTaggedStorage<TKey, TEntry, TMeasureType> : IStorage<TKey, TEntry>, IStorageSelector<TKey, TEntry>, ITypedStorage<TEntry, TMeasureType>, IDisposable
      where TEntry : IAggregatableEntry, IInfluxEntry, new()
      where TMeasureType : IMeasureType
   {
      public const int DefaultReadParallelism = 20;
      public const int DefaultWriteParallelism = 5;

      private readonly StorageSelection<TKey, TEntry, IStorage<TKey, TEntry>>[] _defaultSelection;
      private readonly DateTime _maxTo = new DateTime( 2050, 1, 1, 0, 0, 0, DateTimeKind.Utc );
      private readonly object _sync = new object();
      private readonly InfluxClient _client;
      private readonly string _database;
      private readonly IKeyConverter<TKey> _keyConverter;
      private readonly IConcurrencyControl _cc;
      private readonly ITypedKeyStorage<TKey, TMeasureType> _typeStorage;

      private Task _createDatabase;

      public InfluxTaggedStorage( Uri endpoint, string database, string username, string password, IConcurrencyControl concurrency, IKeyConverter<TKey> keyConverter, ITypedKeyStorage<TKey, TMeasureType> typeStorage )
      {
         _client = new InfluxClient( endpoint, username, password );
         _database = database;

         _client.DefaultQueryOptions.Precision = TimestampPrecision.Nanosecond;
         _client.DefaultWriteOptions.Precision = TimestampPrecision.Nanosecond;
         
         _keyConverter = keyConverter;
         _cc = concurrency;
         _typeStorage = typeStorage;

         _defaultSelection = new[] { new StorageSelection<TKey, TEntry, IStorage<TKey, TEntry>>( this ) };
      }

      public InfluxTaggedStorage( Uri endpoint, string database, string username, string password, IKeyConverter<TKey> keyConverter, ITypedKeyStorage<TKey, TMeasureType> typeStorage )
         : this( endpoint, database, username, password, new ConcurrencyControl( DefaultReadParallelism, DefaultWriteParallelism ), keyConverter, typeStorage )
      {
      }

      public InfluxTaggedStorage( Uri endpoint, string database, string username, string password, IConcurrencyControl concurrency, ITypedKeyStorage<TKey, TMeasureType> typeStorage )
         : this( endpoint, database, username, password, concurrency, DefaultKeyConverter<TKey>.Current, typeStorage )
      {
      }

      public InfluxTaggedStorage( Uri endpoint, string database, string username, string password, ITypedKeyStorage<TKey, TMeasureType> typeStorage )
         : this( endpoint, database, username, password, DefaultKeyConverter<TKey>.Current, typeStorage )
      {
      }

      public InfluxTaggedStorage( Uri endpoint, string database, IConcurrencyControl concurrency, ITypedKeyStorage<TKey, TMeasureType> typeStorage )
         : this( endpoint, database, null, null, concurrency, DefaultKeyConverter<TKey>.Current, typeStorage )
      {

      }

      public InfluxTaggedStorage( Uri endpoint, string database, ITypedKeyStorage<TKey, TMeasureType> typeStorage )
         : this( endpoint, database, null, null, DefaultKeyConverter<TKey>.Current, typeStorage )
      {

      }

      public IEnumerable<StorageSelection<TKey, TEntry, IStorage<TKey, TEntry>>> GetStorage( TKey id, DateTime? from, DateTime? to )
      {
         return _defaultSelection;
      }

      public IStorage<TKey, TEntry> GetStorage( TKey key, TEntry entry )
      {
         return this;
      }

      public async Task WriteAsync( IEnumerable<ISerie<TKey, TEntry>> series )
      {
         using( await _cc.WriteAsync().ConfigureAwait( false ) )
         {
            series = series.ToList(); // ensure we only iterate once
            var keys = await _typeStorage.GetTaggedKeysOrThrowAsync( series.Select( x => x.GetKey() ) ).ConfigureAwait( false );
            var keyDictionary = keys.ToDictionary( x => x.Key );

            await CreateDatabase().ConfigureAwait( false );
            await _client.WriteAsync( _database, Convert( series, keyDictionary ) ).ConfigureAwait( false );
         }
      }

      public async Task DeleteAsync( IEnumerable<TKey> ids, DateTime from, DateTime to )
      {
         using( await _cc.WriteAsync().ConfigureAwait( false ) )
         {
            await CreateDatabase().ConfigureAwait( false );
            var keys = await _typeStorage.GetTaggedKeysOrThrowAsync( ids ).ConfigureAwait( false );
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateDeleteQuery( keys, from, to ) ).ConfigureAwait( false );
         }
      }

      public async Task DeleteAsync( IEnumerable<TKey> ids, DateTime to )
      {
         using( await _cc.WriteAsync().ConfigureAwait( false ) )
         {
            await CreateDatabase().ConfigureAwait( false );
            var keys = await _typeStorage.GetTaggedKeysOrThrowAsync( ids ).ConfigureAwait( false );
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateDeleteQuery( keys, to ) ).ConfigureAwait( false );
         }
      }

      public async Task DeleteAsync( IEnumerable<TKey> ids )
      {
         using( await _cc.WriteAsync().ConfigureAwait( false ) )
         {
            await CreateDatabase().ConfigureAwait( false );
            var keys = await _typeStorage.GetTaggedKeysOrThrowAsync( ids ).ConfigureAwait( false );
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateDeleteQuery( keys ) ).ConfigureAwait( false );
         }
      }

      private async Task DeleteInternalAsync( ITypedKey<TKey, TMeasureType> id, DateTime from, DateTime to )
      {
         using( await _cc.WriteAsync().ConfigureAwait( false ) )
         {
            await CreateDatabase().ConfigureAwait( false );
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateDeleteQuery( new[] { id }, from, to ) ).ConfigureAwait( false );
         }
      }

      public async Task<MultiTaggedReadResult<TEntry, TMeasureType>> ReadGroupsAsync( string measureTypeName, IEnumerable<KeyValuePair<string, string>> requiredTags, IEnumerable<string> groupByTags, GroupMethod groupMethod, Sort sort = Sort.Descending )
      {
         using( await _cc.ReadAsync().ConfigureAwait( false ) )
         {
            await CreateDatabase().ConfigureAwait( false );
            requiredTags = requiredTags.ToList();
            groupByTags = groupByTags.ToList();
            var measureType = await _typeStorage.GetMeasureTypeAsync( measureTypeName ).ConfigureAwait( false );
            var fields = measureType.GetFields().ToArray();

            var resultSet = await _client.ReadAsync<DynamicInfluxRow>( _database, CreateGroupedSelectQuery( measureType, fields, requiredTags, groupByTags, groupMethod, sort ) ).ConfigureAwait( false );
            return Convert( measureType, fields, resultSet, sort );
         }
      }

      public async Task<MultiTaggedReadResult<TEntry, TMeasureType>> ReadGroupsAsync( string measureTypeName, DateTime to, IEnumerable<KeyValuePair<string, string>> requiredTags, IEnumerable<string> groupByTags, GroupMethod groupMethod, Sort sort = Sort.Descending )
      {
         using( await _cc.ReadAsync().ConfigureAwait( false ) )
         {
            await CreateDatabase().ConfigureAwait( false );
            requiredTags = requiredTags.ToList();
            groupByTags = groupByTags.ToList();
            var measureType = await _typeStorage.GetMeasureTypeAsync( measureTypeName ).ConfigureAwait( false );
            var fields = measureType.GetFields().ToArray();

            var resultSet = await _client.ReadAsync<DynamicInfluxRow>( _database, CreateGroupedSelectQuery( measureType, fields, to, requiredTags, groupByTags, groupMethod, sort ) ).ConfigureAwait( false );
            return Convert( measureType, fields, resultSet, sort );
         }
      }

      public async Task<MultiTaggedReadResult<TEntry, TMeasureType>> ReadGroupsAsync( string measureTypeName, DateTime from, DateTime to, IEnumerable<KeyValuePair<string, string>> requiredTags, IEnumerable<string> groupByTags, GroupMethod groupMethod, Sort sort = Sort.Descending )
      {
         using( await _cc.ReadAsync().ConfigureAwait( false ) )
         {
            await CreateDatabase().ConfigureAwait( false );
            requiredTags = requiredTags.ToList();
            groupByTags = groupByTags.ToList();
            var measureType = await _typeStorage.GetMeasureTypeAsync( measureTypeName ).ConfigureAwait( false );
            var fields = measureType.GetFields().ToArray();

            var resultSet = await _client.ReadAsync<DynamicInfluxRow>( _database, CreateGroupedSelectQuery( measureType, fields, from, to, requiredTags, groupByTags, groupMethod, sort ) ).ConfigureAwait( false );
            return Convert( measureType, fields, resultSet, sort );
         }
      }

      public async Task<MultiReadResult<TKey, TEntry>> ReadLatestAsync( IEnumerable<TKey> ids, int count )
      {
         using( await _cc.ReadAsync().ConfigureAwait( false ) )
         {
            await CreateDatabase().ConfigureAwait( false );
            List<TKey> keys = ids.ToList();

            var typedKeys = await _typeStorage.GetTaggedKeysOrThrowAsync( keys ).ConfigureAwait( false );
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateLatestSelectQuery( typedKeys, count ) ).ConfigureAwait( false );
            return Convert( keys, resultSet, Sort.Descending );
         }
      }

      public async Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, Sort sort = Sort.Descending )
      {
         using( await _cc.ReadAsync().ConfigureAwait( false ) )
         {
            await CreateDatabase().ConfigureAwait( false );
            List<TKey> keys = ids.ToList();

            var typedKeys = await _typeStorage.GetTaggedKeysOrThrowAsync( keys ).ConfigureAwait( false );
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateSelectQuery( typedKeys, sort ) ).ConfigureAwait( false );
            return Convert( keys, resultSet, sort );
         }
      }

      public async Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, DateTime to, Sort sort = Sort.Descending )
      {
         using( await _cc.ReadAsync().ConfigureAwait( false ) )
         {
            await CreateDatabase().ConfigureAwait( false );
            List<TKey> keys = ids.ToList();

            var typedKeys = await _typeStorage.GetTaggedKeysOrThrowAsync( keys ).ConfigureAwait( false );
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateSelectQuery( typedKeys, to, sort ) ).ConfigureAwait( false );
            return Convert( keys, resultSet, sort );
         }
      }

      public async Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, DateTime from, DateTime to, Sort sort = Sort.Descending )
      {
         using( await _cc.ReadAsync().ConfigureAwait( false ) )
         {
            await CreateDatabase().ConfigureAwait( false );
            List<TKey> keys = ids.ToList();

            var typedKeys = await _typeStorage.GetTaggedKeysOrThrowAsync( keys ).ConfigureAwait( false );
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateSelectQuery( typedKeys, from, to, sort ) ).ConfigureAwait( false );
            return Convert( keys, resultSet, sort );
         }
      }

      public async Task<SegmentedReadResult<TKey, TEntry>> ReadSegmentedAsync( TKey id, DateTime? from, DateTime? to, int segmentSize, IContinuationToken continuationToken )
      {
         using( await _cc.ReadAsync().ConfigureAwait( false ) )
         {
            await CreateDatabase().ConfigureAwait( false );
            var token = (ContinuationToken)continuationToken;
            to = token?.At ?? to;
            var key = await _typeStorage.GetTaggedKeyOrThrowAsync( id ).ConfigureAwait( false );
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateSegmentedSelectQuery( key, from, to, segmentSize, false, token == null ) ).ConfigureAwait( false );
            bool hasMore = resultSet.Results.FirstOrDefault()?.Series.FirstOrDefault()?.Rows.Count == segmentSize;
            return Convert( key, resultSet, segmentSize );
         }
      }

      public async Task<SegmentedReadResult<TKey, TEntry>> ReadReverseSegmentedAsync( TKey id, DateTime? from, DateTime? to, int segmentSize, IContinuationToken continuationToken )
      {
         using( await _cc.ReadAsync().ConfigureAwait( false ) )
         {
            await CreateDatabase().ConfigureAwait( false );
            var token = (ContinuationToken)continuationToken;
            from = token?.At ?? from;
            var key = await _typeStorage.GetTaggedKeyOrThrowAsync( id ).ConfigureAwait( false );
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateSegmentedSelectQuery( key, from, to, segmentSize, true, token == null ) ).ConfigureAwait( false );
            bool hasMore = resultSet.Results.FirstOrDefault()?.Series.FirstOrDefault()?.Rows.Count == segmentSize;
            return Convert( key, resultSet, segmentSize );
         }
      }

      private string CreateSegmentedSelectQuery( ITypedKey<TKey, TMeasureType> id, DateTime? from, DateTime? to, int take, bool reverse, bool isFirstSegment )
      {
         var key = id.Key;
         var measureType = id.GetMeasureType();
         if( !reverse || isFirstSegment )
         {
            if( from.HasValue && to.HasValue )
            {
               return $"SELECT {CreateDefaultFieldQuery( measureType.GetFields() )} FROM \"{measureType.GetName()}\" WHERE {CreateDefaultTagFilter( id.Key )} AND '{from.Value.ToIso8601()}' <= time AND time < '{to.Value.ToIso8601()}' ORDER BY time {( reverse ? "ASC" : "DESC" )} LIMIT {take}";
            }
            else if( !from.HasValue && to.HasValue )
            {
               return $"SELECT {CreateDefaultFieldQuery( measureType.GetFields() )} FROM \"{measureType.GetName()}\" WHERE {CreateDefaultTagFilter( id.Key )} AND time < '{to.Value.ToIso8601()}' ORDER BY time {( reverse ? "ASC" : "DESC" )} LIMIT {take}";
            }
            else if( from.HasValue && !to.HasValue )
            {
               return $"SELECT {CreateDefaultFieldQuery( measureType.GetFields() )} FROM \"{measureType.GetName()}\" WHERE {CreateDefaultTagFilter( id.Key )} AND '{from.Value.ToIso8601()}' <= time AND time < '{_maxTo.ToIso8601()}' ORDER BY time {( reverse ? "ASC" : "DESC" )} LIMIT {take}";
            }
            else
            {
               return $"SELECT {CreateDefaultFieldQuery( measureType.GetFields() )} FROM \"{measureType.GetName()}\" WHERE {CreateDefaultTagFilter( id.Key )} AND time < '{_maxTo.ToIso8601()}' ORDER BY time {( reverse ? "ASC" : "DESC" )} LIMIT {take}";
            }
         }
         else
         {
            if( from.HasValue && to.HasValue )
            {
               return $"SELECT {CreateDefaultFieldQuery( measureType.GetFields() )} FROM \"{measureType.GetName()}\" WHERE {CreateDefaultTagFilter( id.Key )} AND '{from.Value.ToIso8601()}' < time AND time < '{to.Value.ToIso8601()}' ORDER BY time {( reverse ? "ASC" : "DESC" )} LIMIT {take}";
            }
            else if( !from.HasValue && to.HasValue )
            {
               return $"SELECT {CreateDefaultFieldQuery( measureType.GetFields() )} FROM \"{measureType.GetName()}\" WHERE {CreateDefaultTagFilter( id.Key )} AND time < '{to.Value.ToIso8601()}' ORDER BY time {( reverse ? "ASC" : "DESC" )} LIMIT {take}";
            }
            else if( from.HasValue && !to.HasValue )
            {
               return $"SELECT {CreateDefaultFieldQuery( measureType.GetFields() )} FROM \"{measureType.GetName()}\" WHERE {CreateDefaultTagFilter( id.Key )} AND '{from.Value.ToIso8601()}' < time AND time < '{_maxTo.ToIso8601()}' ORDER BY time {( reverse ? "ASC" : "DESC" )} LIMIT {take}";
            }
            else
            {
               return $"SELECT {CreateDefaultFieldQuery( measureType.GetFields() )} FROM \"{measureType.GetName()}\" WHERE {CreateDefaultTagFilter( id.Key )} AND time < '{_maxTo.ToIso8601()}' ORDER BY time {( reverse ? "ASC" : "DESC" )} LIMIT {take}";
            }
         }
      }

      private string CreateLatestSelectQuery( IEnumerable<ITypedKey<TKey, TMeasureType>> ids, int count )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            var measureType = id.GetMeasureType();
            sb.Append( $"SELECT {CreateDefaultFieldQuery( measureType.GetFields() )} FROM \"{measureType.GetName()}\" WHERE {CreateDefaultTagFilter( id.Key )} AND time < '{_maxTo.ToIso8601()}' ORDER BY time DESC LIMIT {count};" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string CreateSelectQuery( IEnumerable<ITypedKey<TKey, TMeasureType>> ids, DateTime from, DateTime to, Sort sort )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            var measureType = id.GetMeasureType();
            sb.Append( $"SELECT {CreateDefaultFieldQuery( measureType.GetFields() )} FROM \"{measureType.GetName()}\" WHERE {CreateDefaultTagFilter( id.Key )} AND '{from.ToIso8601()}' <= time AND time < '{to.ToIso8601()}' ORDER BY time {GetQueryPart( sort )};" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string CreateSelectQuery( IEnumerable<ITypedKey<TKey, TMeasureType>> ids, DateTime to, Sort sort )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            var measureType = id.GetMeasureType();
            sb.Append( $"SELECT {CreateDefaultFieldQuery( measureType.GetFields() )} FROM \"{measureType.GetName()}\" WHERE {CreateDefaultTagFilter( id.Key )} AND time < '{to.ToIso8601()}' ORDER BY time {GetQueryPart( sort )};" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string CreateSelectQuery( IEnumerable<ITypedKey<TKey, TMeasureType>> ids, Sort sort )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            var measureType = id.GetMeasureType();
            sb.Append( $"SELECT {CreateDefaultFieldQuery( measureType.GetFields() )} FROM \"{measureType.GetName()}\" WHERE {CreateDefaultTagFilter( id.Key )} AND time < '{_maxTo.ToIso8601()}' ORDER BY time {GetQueryPart( sort )};" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string CreateGroupedSelectQuery( TMeasureType measureType, IFieldInfo[] fields, DateTime from, DateTime to, IEnumerable<KeyValuePair<string, string>> requiredTags, IEnumerable<string> groupByTags, GroupMethod groupMethod, Sort sort )
      {
         StringBuilder sb = new StringBuilder();
         sb.Append( $"SELECT {CreateFieldQuery( fields, groupMethod )} FROM \"{measureType.GetName()}\" WHERE {CreateTagFilter( requiredTags )}{( requiredTags.Any() ? " AND" : "" )} '{from.ToIso8601()}' <= time AND time < '{to.ToIso8601()}' GROUP BY {CreateGroupBy( groupByTags )} ORDER BY time {GetQueryPart( sort )}" );
         return sb.ToString();
      }

      private string CreateGroupedSelectQuery( TMeasureType measureType, IFieldInfo[] fields, DateTime to, IEnumerable<KeyValuePair<string, string>> requiredTags, IEnumerable<string> groupByTags, GroupMethod groupMethod, Sort sort )
      {
         StringBuilder sb = new StringBuilder();
         sb.Append( $"SELECT {CreateFieldQuery( fields, groupMethod )} FROM \"{measureType.GetName()}\" WHERE {CreateTagFilter( requiredTags )}{( requiredTags.Any() ? " AND" : "" )} time < '{to.ToIso8601()}' GROUP BY {CreateGroupBy( groupByTags )} ORDER BY time {GetQueryPart( sort )}" );
         return sb.ToString();
      }

      private string CreateGroupedSelectQuery( TMeasureType measureType, IFieldInfo[] fields, IEnumerable<KeyValuePair<string, string>> requiredTags, IEnumerable<string> groupByTags, GroupMethod groupMethod, Sort sort )
      {
         StringBuilder sb = new StringBuilder();
         sb.Append( $"SELECT {CreateFieldQuery( fields, groupMethod )} FROM \"{measureType.GetName()}\" WHERE {CreateTagFilter( requiredTags )}{( requiredTags.Any() ? " AND" : "" )} time < '{_maxTo.ToIso8601()}' GROUP BY {CreateGroupBy( groupByTags )} ORDER BY time {GetQueryPart( sort )}" );
         return sb.ToString();
      }

      private string CreateDeleteQuery( IEnumerable<ITypedKey<TKey, TMeasureType>> ids, DateTime from, DateTime to )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            sb.Append( $"DELETE FROM \"{id.GetMeasureType().GetName()}\" WHERE {CreateDefaultTagFilter( id.Key )} AND '{from.ToIso8601()}' <= time AND time < '{to.ToIso8601()}';" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string CreateDeleteQuery( IEnumerable<ITypedKey<TKey, TMeasureType>> ids, DateTime to )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            sb.Append( $"DELETE FROM \"{id.GetMeasureType().GetName()}\" WHERE {CreateDefaultTagFilter( id.Key )} AND time < '{to.ToIso8601()}';" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string CreateDeleteQuery( IEnumerable<ITypedKey<TKey, TMeasureType>> ids )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            sb.Append( $"DELETE FROM \"{id.GetMeasureType().GetName()}\" WHERE {CreateDefaultTagFilter( id.Key )};" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string CreateDefaultFieldQuery( IEnumerable<IFieldInfo> fields )
      {
         return string.Join( ", ", fields.Select( field => "\"" + field.Key + "\"" ) );
      }

      private string CreateDefaultTagFilter( TKey key )
      {
         return $"\"{ReservedNames.UniqueId}\" = '{_keyConverter.Convert( key )}'";
      }

      private string CreateFieldQuery( IEnumerable<IFieldInfo> fields, GroupMethod groupMethod )
      {
         IFieldInfo fieldToCount = null;
         var aggregate = GetQueryPart( groupMethod );
         var sb = new StringBuilder();
         foreach( var field in fields )
         {
            fieldToCount = field;
            sb.Append( aggregate );
            sb.Append( "(\"" );
            sb.Append( field.Key );
            sb.Append( "\") AS " );
            sb.Append( field.Key );
            sb.Append( ", " );
         }
         sb.Append( "COUNT(\"" );
         sb.Append( fieldToCount.Key );
         sb.Append( "\") AS " );
         sb.Append( ReservedNames.Count );

         return sb.ToString();
      }

      private string CreateTagFilter( IEnumerable<KeyValuePair<string, string>> requiredTags )
      {
         return string.Join( " AND ", requiredTags.Select( kvp => "\"" + kvp.Key + "\" = '" + kvp.Value + "'" ) );
      }

      private string CreateGroupBy( IEnumerable<string> groupByTags )
      {
         var sb = new StringBuilder();
         sb.Append( "time(1s)" ); // TODO: configurable value here?
         if( groupByTags.Any() )
         {
            sb.Append( ", " );
            sb.Append( string.Join( ", ", groupByTags.Select( tag => "\"" + tag + "\"" ) ) );
         }
         sb.Append( " fill(none)" );
         return sb.ToString();
      }

      private string GetQueryPart( GroupMethod groupMethod )
      {
         switch( groupMethod )
         {
            case GroupMethod.Average:
               return "MEAN";
            case GroupMethod.Sum:
               return "SUM";
            case GroupMethod.Min:
               return "MIN";
            case GroupMethod.Max:
               return "MAX";
            default:
               throw new ArgumentException( "Invalid group method specified.", nameof( groupMethod ) );
         }
      }

      private string GetQueryPart( Sort sort )
      {
         if( sort == Sort.Ascending )
         {
            return "ASC";
         }
         else
         {
            return "DESC";
         }
      }

      private MultiTaggedReadResult<TEntry, TMeasureType> Convert( TMeasureType measureType, IFieldInfo[] fields, InfluxResultSet<DynamicInfluxRow> resultSet, Sort sort )
      {
         var output = new Dictionary<TagCollection, TaggedReadResult<TEntry, TMeasureType>>();

         var measureTypeName = measureType.GetName();
         var result = resultSet.Results.FirstOrDefault();
         if( result != null )
         {
            foreach( var serie in result.Series )
            {
               List<TEntry> entries = new List<TEntry>();

               foreach( var row in serie.Rows )
               {
                  // move all fields to new entry
                  IAggregatableEntry entry = new TEntry();
                  entry.SetTimestamp( row.GetTimestamp().Value );
                  entry.SetCount( (int)(long)row.GetField( ReservedNames.Count ) );
                  for( int i = 0 ; i < fields.Length ; i++ )
                  {
                     var name = fields[ i ].Key;
                     entry.SetField( name, row.GetField( name ) );
                  }
                  entries.Add( (TEntry)entry );
               }

               var tags = new TagCollection( serie.GroupedTags.ToDictionary( x => x.Key, x => (string)x.Value ) );
               var taggedReadResiæt = new TaggedReadResult<TEntry, TMeasureType>( tags, sort, entries );

               output.Add( tags, taggedReadResiæt );
            }
         }

         return new MultiTaggedReadResult<TEntry, TMeasureType>( measureType, output );
      }

      private MultiReadResult<TKey, TEntry> Convert( List<TKey> requiredIds, InfluxResultSet<TEntry> resultSet, Sort sort )
      {
         IDictionary<string, ReadResult<TKey, TEntry>> results = new Dictionary<string, ReadResult<TKey, TEntry>>();
         foreach( var id in requiredIds )
         {
            results[ _keyConverter.Convert( id ) ] = new ReadResult<TKey, TEntry>( id, sort );
         }

         for( int i = 0 ; i < requiredIds.Count ; i++ )
         {
            var id = requiredIds[ i ];
            var result = resultSet.Results.FirstOrDefault( x => x.StatementId == i );
            if( result != null )
            {
               var serie = result.Series.FirstOrDefault();
               if( serie != null )
               {
                  results[ _keyConverter.Convert( id ) ] = new ReadResult<TKey, TEntry>( id, sort, serie.Rows );
               }
            }
         }

         return new MultiReadResult<TKey, TEntry>( results.Values.ToDictionary( x => x.Key ) );
      }

      private IEnumerable<TypedInfluxEntryAdapter<TKey, TEntry, TMeasureType>> Convert(
         IEnumerable<ISerie<TKey, TEntry>> series,
         IDictionary<TKey, ITypedKey<TKey, TMeasureType>> keyDictionary )
      {
         var keys = new HashSet<EntryKey<TKey>>();
         foreach( var serie in series )
         {
            var key = serie.GetKey();
            var id = _keyConverter.Convert( key );

            ITypedKey<TKey, TMeasureType> typedKey;
            if( keyDictionary.TryGetValue( key, out typedKey ) )
            {
               var measureType = typedKey.GetMeasureType();
               var measurementName = measureType.GetName();

               // create tag dictionary per type (not per entry!)
               Dictionary<string, string> additionalTags = new Dictionary<string, string>();
               foreach( var tagName in measureType.GetTags() )
               {
                  var tagValue = typedKey.GetTagValue( tagName );
                  if( tagValue != null )
                  {
                     additionalTags.Add( tagName, tagValue );
                  }
               }

               foreach( var entry in serie.GetEntries() )
               {
                  var hashkey = new EntryKey<TKey>( key, ( (IEntry)entry ).GetTimestamp() );
                  if( !keys.Contains( hashkey ) )
                  {
                     yield return new TypedInfluxEntryAdapter<TKey, TEntry, TMeasureType>( measurementName, id, additionalTags, entry );
                     keys.Add( hashkey );
                  }
               }
            }
            else
            {
               throw new TsdbException( $"Could not find type information for measure point with key '{id}'." );
            }
         }
      }

      private SegmentedReadResult<TKey, TEntry> Convert( ITypedKey<TKey, TMeasureType> id, InfluxResultSet<TEntry> resultSet, int segmentSize )
      {
         var list = resultSet.Results.FirstOrDefault()?.Series.FirstOrDefault()?.Rows;
         var entries = list;
         DateTime? to = null;
         if( entries.Count > 0 )
         {
            to = ( (IEntry)entries[ entries.Count - 1 ] ).GetTimestamp();
         }
         var continuationToken = new ContinuationToken( entries.Count == segmentSize, to );

         return new SegmentedReadResult<TKey, TEntry>( id.Key, Sort.Descending, continuationToken, entries, CreateDeleteFunction( id, continuationToken, entries ) );
      }

      private Func<Task> CreateDeleteFunction( ITypedKey<TKey, TMeasureType> key, ContinuationToken token, List<TEntry> entries )
      {
         if( entries.Count == 0 )
         {
            return () => Task.FromResult( 0 );
         }
         else
         {
            DateTime from;
            DateTime to;

            var ts1 = ( (IEntry)entries[ 0 ] ).GetTimestamp();
            var ts2 = ( (IEntry)entries[ entries.Count - 1 ] ).GetTimestamp();
            if( ts1 >= ts2 )
            {
               to = ts1;
               from = ts2;
            }
            else
            {
               to = ts2;
               from = ts1;
            }

            to = to.AddTicks( 1 );

            // TODO: Implement DeleteInternalAsync
            return () => this.DeleteInternalAsync( key, from, to );
         }
      }

      private Task CreateDatabase()
      {
         lock( _sync )
         {
            if( _createDatabase == null || _createDatabase.IsFaulted || _createDatabase.IsCanceled )
            {
               _createDatabase = _client.CreateDatabaseAsync( _database );
            }
         }
         return _createDatabase;
      }

      #region IDisposable Support

      private bool _dispose = false; // To detect redundant calls

      protected virtual void Dispose( bool disposing )
      {
         if( !_dispose )
         {
            if( disposing )
            {
               _client.Dispose();
            }

            _dispose = true;
         }
      }

      // This code added to correctly implement the disposable pattern.
      public void Dispose()
      {
         Dispose( true );
      }

      #endregion
   }
}
