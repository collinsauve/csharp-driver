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
    public class ConsistencyTests
    {
        private const ConsistencyLevel CL = ConsistencyLevel.LocalQuorum;
        private const string ConnectionString = "Contact Points=10.200.3.11,10.200.3.12";

        [Cassandra.Mapping.Attributes.Table("TestEntity1")]
        public class TestEntity1
        {
            [Cassandra.Mapping.Attributes.PartitionKey]
            [Cassandra.Mapping.Attributes.Column("PartitionKey")]
            public string PartitionKey { get; set; }
            [Cassandra.Mapping.Attributes.ClusteringKey]
            [Cassandra.Mapping.Attributes.Column("ClusteringKey")]
            public string ClusteringKey { get; set; }
            [Cassandra.Mapping.Attributes.Column("Field")]
            public string Field { get; set; }
        }
        
        [Test]
        public async Task TestConsistency()
        {
            var session = Connect();
            var table = new Table<TestEntity1>(session);
            table.CreateIfNotExists();
            var mapper = new Mapper(session);
            var queryOptions = CqlQueryOptions.New().SetConsistencyLevel(CL).EnableTracing();

            await Loop(async () =>
            {
                var item = new TestEntity1
                {
                    PartitionKey = Guid.NewGuid().ToString(),
                    ClusteringKey = Guid.NewGuid().ToString(),
                    Field = Guid.NewGuid().ToString(),
                };

                // Insert 
                await mapper.InsertAsync(item, queryOptions);

                // Delete
                await mapper.DeleteAsync(item, queryOptions);

                // Was it deleted?
                var results = await Fetch(table, item);
                Assert.AreEqual(0, results.Count, string.Format("Expected 0 records but found {0}", results.Count));
            });
        }

        private static async Task<IList<TestEntity1>> Fetch(Table<TestEntity1> table, TestEntity1 item)
        {
            var results = await table.Where(_ => _.PartitionKey == item.PartitionKey).SetConsistencyLevel(CL).ExecuteAsync();
            return results.ToList();
        }

        private static async Task Loop(Func<Task> fn)
        {
            var failures = 0;
            foreach (var i in Enumerable.Range(0, 10))
            {
                try
                {
                    await fn();
                    Console.WriteLine("Succeeded Run #" + i);
                }
                catch (AssertionException ex)
                {
                    Console.WriteLine("Failed Run #" + i + " " + ex.Message);
                    failures++;
                }
            }
            Assert.AreEqual(0, failures);
        }

        private static ISession Connect()
        {
            var anotherKeyspace = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            var cluster = Cluster.Builder()
                                 .WithConnectionString(ConnectionString)
                                 .WithSocketOptions(new SocketOptions().SetConnectTimeoutMillis(30000))
                                 .Build();
            var session = cluster.Connect();
            session.CreateKeyspace(anotherKeyspace, new Dictionary<string, string>
            {
                {"class", "SimpleStrategy"},
                {"replication_factor", "2"}
            });
            session.ChangeKeyspace(anotherKeyspace);
            return session;
        }
    }
}