using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Mapping.Tests
{
    [Category("short")]
    public class BatchConsistencyTests : SharedClusterTest
    {
        public BatchConsistencyTests()
            : base(3, true, false)
        {
        }

        public class TestEntity1 : IEquatable<TestEntity1>
        {

            public string PartitionKey { get; set; }
            public string ClusteringKey { get; set; }
            public string Field { get; set; }

            public override int GetHashCode()
            {
                return 0;
            }

            public override bool Equals(object obj)
            {
                var cast = obj as TestEntity1;
                return cast != null && Equals(cast);
            }

            public bool Equals(TestEntity1 other)
            {
                return other != null && PartitionKey.Equals(other.PartitionKey) && ClusteringKey.Equals(other.ClusteringKey) && Field.Equals(other.Field);
            }

            public override string ToString()
            {
                return "PartitionKey=" + PartitionKey + ", ClusteringKey=" + ClusteringKey + ", Field=" + Field;
            }
        }

        [Test]
        [TestCase("Contact Points=127.0.0.1")] // "CCM, single contact point"
        [TestCase("Contact Points=127.0.0.1,127.0.0.2,127.0.0.3")] // "CCM, multiple contact points"
        [TestCase("Contact Points=10.200.3.11,10.200.3.12")] // "CI"
        [TestCase("Contact Points=10.200.3.4,10.200.3.5,10.200.3.6")] // "QA"
        public async Task TestBatchConsistency(string connectionString)
        {
            const int nItems = 10;
            const int times = 3;  // Run the test multiple times

            var anotherKeyspace = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            var cluster = Cluster.Builder()
                                 .WithConnectionString(connectionString)
                                 .WithSocketOptions(new SocketOptions().SetConnectTimeoutMillis(30000))
                                 .Build();
            var session = cluster.Connect();
            session.CreateKeyspace(anotherKeyspace, new Dictionary<string, string>
            {
                {"class", "SimpleStrategy"},
                {"replication_factor", "2"}
            });
            session.ChangeKeyspace(anotherKeyspace);

            var config = new MappingConfiguration().Define(new Map<TestEntity1>()
                .ExplicitColumns()
                .PartitionKey(_ => _.PartitionKey)
                .ClusteringKey(_ => _.ClusteringKey)
                .Column(_ => _.PartitionKey, c => c.WithDbType<string>())
                .Column(_ => _.ClusteringKey, c => c.WithDbType<string>())
                .Column(_ => _.Field, c => c.WithDbType<string>())
                .TableName("batch_consistency"));

            var table = new Table<TestEntity1>(session, config);
            table.CreateIfNotExists();
            var mapper = new Mapper(session, config);

            var failures = 0;
            foreach (var i in Enumerable.Range(0, times))
            {
                try
                {
                    await TestRawOnce(table, mapper, nItems);
                    Console.WriteLine("Run #" + i);
                }
                catch (AssertionException ex)
                {
                    Console.WriteLine("Run #" + i + " " + ex.Message);
                    failures++;
                }
            }
            Assert.AreEqual(0, failures);
        }

        private static async Task TestRawOnce(CqlQuery<TestEntity1> table, IMapper mapper, int nItems)
        {
            var queryOptions = CqlQueryOptions.New().SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            var pk = Guid.NewGuid().ToString();
            var items = Enumerable.Range(0, nItems).Select(n => GenerateItem(pk)).OrderBy(s => s.ClusteringKey).ToList();

            // Insert all items in batch
            var insertBatch = mapper.CreateBatch();
            foreach (var item in items)
            {
                insertBatch.Insert(item, false, 10000, queryOptions);
            }
            insertBatch.WithOptions(o => o.SetConsistencyLevel(ConsistencyLevel.LocalQuorum));
            await mapper.ExecuteAsync(insertBatch);

            // Now fetch these and assert they were all inserted
            await AssertExpected(table, pk, items, "insert");

            // Update the field on each item
            items.ForEach(_ => _.Field = Guid.NewGuid().ToString());
            var updateBatch = mapper.CreateBatch();
            foreach (var item in items)
            {
                updateBatch.Update(item, queryOptions);
            }
            updateBatch.WithOptions(o => o.SetConsistencyLevel(ConsistencyLevel.LocalQuorum));
            await mapper.ExecuteAsync(updateBatch);

            // Now fetch again and assert they were all updated
            await AssertExpected(table, pk, items, "update");

            // Delete all items in batch
            var deleteBatch = mapper.CreateBatch();
            foreach (var item in items)
            {
                deleteBatch.Delete(item, queryOptions);
            }
            deleteBatch.WithOptions(o => o.SetConsistencyLevel(ConsistencyLevel.LocalQuorum));
            await mapper.ExecuteAsync(deleteBatch);

            // Now fetch again and assert they were all deleted
            await AssertExpected(table, pk, new List<TestEntity1>(), "delete");
        }

        private static async Task AssertExpected(CqlQuery<TestEntity1> table, string pk, List<TestEntity1> expected, string step)
        {
            var cql = table.Where(_ => _.PartitionKey == pk).SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            var results = await cql.ExecuteAsync();
            var sortedResults = results.OrderBy(_ => _.ClusteringKey).ToList();
            Assert.That(sortedResults, Is.EquivalentTo(expected), "Failed after " + step + " step");
        }

        private static TestEntity1 GenerateItem(string pk)
        {
            return new TestEntity1
            {
                PartitionKey = pk,
                ClusteringKey = Guid.NewGuid().ToString(),
                Field = Guid.NewGuid().ToString(),
            };
        }
    }
}