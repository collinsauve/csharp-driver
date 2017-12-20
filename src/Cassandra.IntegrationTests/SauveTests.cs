using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using NUnit.Framework;

namespace Cassandra.IntegrationTests
{
    [TestFixture]
    public class SauveTests
    {
        private const string ConnectionString = "Contact Points=127.0.0.1";
        private ISession _session;

        public class TestUdtWithLong
        {
            public long UdtField { get; set; }
        }

        public class TestUdtWithNullableLong
        {
            public long? UdtField { get; set; }
        }

        [Cassandra.Mapping.Attributes.Table("table_with_udt_with_long")]
        public class TestTableWithUdtWithLong
        {
            [Cassandra.Mapping.Attributes.PartitionKey]
            [Cassandra.Mapping.Attributes.Column("pk")]
            public string PartitionKey { get; set; }

            [Cassandra.Mapping.Attributes.Column("udt_field")]
            public IEnumerable<TestUdtWithLong> UdtField { get; set; }
        }

        [Cassandra.Mapping.Attributes.Table("table_with_udt_with_nullable_long")]
        public class TestTableWithUdtWithNullableLong
        {
            [Cassandra.Mapping.Attributes.PartitionKey]
            [Cassandra.Mapping.Attributes.Column("pk")]
            public string PartitionKey { get; set; }

            [Cassandra.Mapping.Attributes.Column("udt_field")]
            public IEnumerable<TestUdtWithNullableLong> UdtField { get; set; }
        }

        [SetUp]
        public void Setup()
        {
            _session = Cluster.Builder().WithConnectionString(ConnectionString).WithDefaultKeyspace("test").Build().ConnectAndCreateDefaultKeyspaceIfNotExists();
            _session.Execute(new SimpleStatement(@"DROP TABLE IF EXISTS test.table_with_udt_with_long;"));
            _session.Execute(new SimpleStatement(@"DROP TABLE IF EXISTS test.table_with_udt_with_nullable_long;"));
            _session.Execute(new SimpleStatement(@"DROP TYPE IF EXISTS udt_with_long;"));
            _session.Execute(new SimpleStatement(@"DROP TYPE IF EXISTS udt_with_nullable_long;"));
            _session.Execute(new SimpleStatement(@"
                CREATE TYPE test.udt_with_long(
                    udt_field bigint
                );
            "));
            _session.Execute(new SimpleStatement(@"
                CREATE TYPE test.udt_with_nullable_long(
                    udt_field bigint
                );
            "));
            _session.Execute(new SimpleStatement(@"
                CREATE TABLE test.table_with_udt_with_long  (
                    pk text,
                    udt_field list<frozen<udt_with_long>>,
                    PRIMARY KEY(pk)
                );
            "));
            _session.Execute(new SimpleStatement(@"
                CREATE TABLE test.table_with_udt_with_nullable_long  (
                    pk text,
                    udt_field list<frozen<udt_with_nullable_long>>,
                    PRIMARY KEY(pk)
                );
            "));
            var udtMapLong = new UdtMap<TestUdtWithLong>("udt_with_long")
                .Map(_ => _.UdtField, "udt_field");
            var udtMapNullableLong = new UdtMap<TestUdtWithNullableLong>("udt_with_nullable_long")
                .Map(_ => _.UdtField, "udt_field");
            _session.UserDefinedTypes.Define(udtMapLong, udtMapNullableLong);
            
        }

        [Test]
        public async Task TestLong()
        {
            var entitySaved = new TestTableWithUdtWithLong
            {
                PartitionKey = Guid.NewGuid().ToString(),
                UdtField = Enumerable.Range(0, 5).Select(_ => new TestUdtWithLong
                {
                    UdtField = _
                }).ToArray()
            };
            var mapper = new Mapper(_session);
            await mapper.InsertAsync(entitySaved, false, 600, new CqlQueryOptions());
            
            var table = new Table<TestTableWithUdtWithLong>(_session);
            var query = table.Where(_ => _.PartitionKey == entitySaved.PartitionKey);
            await query.ExecuteAsync();
        }

        [Test]
        public async Task TestNullableLongWithValue()
        {
            var entitySaved = new TestTableWithUdtWithNullableLong
            {
                PartitionKey = Guid.NewGuid().ToString(),
                UdtField = Enumerable.Range(0, 5).Select(_ => new TestUdtWithNullableLong
                {
                    UdtField = _
                }).ToArray()
            };
            var mapper = new Mapper(_session);
            await mapper.InsertAsync(entitySaved, false, 600, new CqlQueryOptions());

            var table = new Table<TestTableWithUdtWithNullableLong>(_session);
            var query = table.Where(_ => _.PartitionKey == entitySaved.PartitionKey);
            await query.ExecuteAsync();
        }

        [Test]
        public async Task TestNullableLongWithNull()
        {
            var entitySaved = new TestTableWithUdtWithNullableLong
            {
                PartitionKey = Guid.NewGuid().ToString(),
                UdtField = Enumerable.Range(0, 5).Select(_ => new TestUdtWithNullableLong
                {
                    UdtField = null
                }).ToArray()
            };
            var mapper = new Mapper(_session);
            await mapper.InsertAsync(entitySaved, false, 600, new CqlQueryOptions());

            var table = new Table<TestTableWithUdtWithNullableLong>(_session);
            var query = table.Where(_ => _.PartitionKey == entitySaved.PartitionKey);
            await query.ExecuteAsync();
        }
    }
}
