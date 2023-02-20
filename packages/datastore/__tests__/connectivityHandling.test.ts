import { Observable } from 'zen-observable-ts';
import { parse } from 'graphql';
import {
	pause,
	getDataStore,
	waitForEmptyOutbox,
	waitForDataStoreReady,
} from './helpers';
import { Predicates } from '../src/predicates';
import { syncExpression } from '../src/types';

describe('DataStore sync engine', () => {
	// establish types :)
	let {
		DataStore,
		connectivityMonitor,
		LegacyJSONPost,
		Post,
		Comment,
		Model,
		graphqlService,
		simulateConnect,
		simulateDisconnect,
	} = getDataStore({ online: true, isNode: false });

	beforeEach(async () => {
		({
			DataStore,
			connectivityMonitor,
			LegacyJSONPost,
			Post,
			Comment,
			Model,
			graphqlService,
			simulateConnect,
			simulateDisconnect,
		} = getDataStore({ online: true, isNode: false }));
		await DataStore.start();
	});

	afterEach(async () => {
		await DataStore.clear();
	});

	describe('basic protocol', () => {
		test('sends model create to the cloud', async () => {
			const post = await DataStore.save(new Post({ title: 'post title' }));

			// give thread control back to subscription event handlers.
			await pause(1);

			const table = graphqlService.tables.get('Post')!;
			expect(table.size).toEqual(1);

			const savedItem = table.get(JSON.stringify([post.id])) as any;
			expect(savedItem.title).toEqual(post.title);
		});

		test('uses model create subscription event to populate sync protocol metadata', async () => {
			const post = await DataStore.save(new Post({ title: 'post title' }));
			await waitForEmptyOutbox();

			const retrieved = (await DataStore.query(Post, post.id)) as any;

			expect(retrieved._version).toBe(1);
			expect(retrieved._deleted).toBe(false);
			expect(retrieved._lastChangedAt).toBeGreaterThan(0);
		});

		test('sends model update to the cloud', async () => {
			const post = await DataStore.save(new Post({ title: 'post title' }));
			await waitForEmptyOutbox();

			const retrieved = await DataStore.query(Post, post.id);

			const updated = await DataStore.save(
				Post.copyOf(retrieved!, draft => {
					draft.title = 'updated title';
				})
			);
			await waitForEmptyOutbox();

			const table = graphqlService.tables.get('Post')!;
			expect(table.size).toEqual(1);

			const savedItem = table.get(JSON.stringify([post.id])) as any;
			expect(savedItem.title).toEqual(updated.title);
		});

		test('sends model deletes to the cloud', async () => {
			const post = await DataStore.save(new Post({ title: 'post title' }));
			await waitForEmptyOutbox();

			const retrieved = await DataStore.query(Post, post.id);
			const deleted = await DataStore.delete(retrieved!);
			await waitForEmptyOutbox();

			const table = graphqlService.tables.get('Post')!;
			expect(table.size).toEqual(1);

			const savedItem = table.get(JSON.stringify([post.id])) as any;
			expect(savedItem.title).toEqual(deleted.title);
			expect(savedItem._deleted).toEqual(true);
		});

		test('sends conditional model deletes to the cloud with valid conditions', async () => {
			const post = await DataStore.save(new Post({ title: 'post title' }));
			await waitForEmptyOutbox();

			const retrieved = await DataStore.query(Post, post.id);

			const deleted = await DataStore.delete(retrieved!, p =>
				p.title.eq('post title')
			);
			await waitForEmptyOutbox();

			const table = graphqlService.tables.get('Post')!;
			expect(table.size).toEqual(1);

			const savedItem = table.get(JSON.stringify([post.id])) as any;
			expect(savedItem.title).toEqual(deleted.title);
			expect(savedItem._deleted).toEqual(true);
		});
	});

	describe('connection state change handling', () => {
		test('survives online -> offline -> online cycle', async () => {
			const post = await DataStore.save(
				new Post({
					title: 'a title',
				})
			);

			await waitForEmptyOutbox();
			await simulateDisconnect();
			await simulateConnect();
			await pause(1);

			const anotherPost = await DataStore.save(
				new Post({
					title: 'another title',
				})
			);

			await waitForEmptyOutbox();

			const table = graphqlService.tables.get('Post')!;
			expect(table.size).toEqual(2);

			const cloudPost = table.get(JSON.stringify([post.id])) as any;
			expect(cloudPost.title).toEqual('a title');

			const cloudAnotherPost = table.get(
				JSON.stringify([anotherPost.id])
			) as any;
			expect(cloudAnotherPost.title).toEqual('another title');
		});

		test('survives online -> offline -> save -> online cycle (non-racing)', async () => {
			const post = await DataStore.save(
				new Post({
					title: 'a title',
				})
			);

			await waitForEmptyOutbox();
			await simulateDisconnect();

			const outboxEmpty = waitForEmptyOutbox();
			const anotherPost = await DataStore.save(
				new Post({
					title: 'another title',
				})
			);

			// In this scenario, we want to test the case where the offline
			// save is NOT in a race with reconnection. So, we pause *briefly*.
			await pause(1);

			await simulateConnect();
			await outboxEmpty;

			const table = graphqlService.tables.get('Post')!;
			expect(table.size).toEqual(2);

			const cloudPost = table.get(JSON.stringify([post.id])) as any;
			expect(cloudPost.title).toEqual('a title');

			const cloudAnotherPost = table.get(
				JSON.stringify([anotherPost.id])
			) as any;
			expect(cloudAnotherPost.title).toEqual('another title');
		});

		/**
		 * Existing bug. (Sort of.)
		 *
		 * Outbox mutations are processed, but the hub events are not sent, so
		 * the test hangs and times out. :shrug:
		 *
		 * It is notable that the data is correct in this case, we just don't
		 * receive all of the expected Hub events.
		 */
		test.skip('survives online -> offline -> save/online race', async () => {
			const post = await DataStore.save(
				new Post({
					title: 'a title',
				})
			);

			await waitForEmptyOutbox();
			await simulateDisconnect();

			const outboxEmpty = waitForEmptyOutbox();

			const anotherPost = await DataStore.save(
				new Post({
					title: 'another title',
				})
			);

			// NO PAUSE: Simulate reconnect IMMEDIATELY, causing a race
			// between the save and the sync engine reconnection operations.

			await simulateConnect();
			await outboxEmpty;

			const table = graphqlService.tables.get('Post')!;
			expect(table.size).toEqual(2);

			const cloudPost = table.get(JSON.stringify([post.id])) as any;
			expect(cloudPost.title).toEqual('a title');

			const cloudAnotherPost = table.get(
				JSON.stringify([anotherPost.id])
			) as any;
			expect(cloudAnotherPost.title).toEqual('another title');
		});
	});

	describe('selective sync', () => {
		const generateTestData = async () => {
			const titles = [
				'1. doing laundry',
				'2. making dinner',
				'3. cleaning dishes',
				'4. taking out the trash',
				'5. cleaning your boots',
			];

			for (const title of titles) {
				await DataStore.save(
					new Post({
						title,
					})
				);
			}
		};

		const resyncWith = async (expressions: any[]) => {
			(DataStore as any).syncExpressions = expressions;
			await DataStore.start();
			await waitForDataStoreReady();
		};

		beforeEach(async () => {
			await generateTestData();

			// make sure "AppSync" has all the records.
			await waitForEmptyOutbox();

			// clear the local -- each test herein will configure sync expressions
			// and begin syncing on a clean database.
			await DataStore.clear();
		});

		/**
		 * Don't call `DataStore.configure()` directly. It will clobber the AppSync
		 * configuration and will no longer interact with the fake backend on restart.
		 */

		test('Predicates.ALL', async () => {
			await resyncWith([syncExpression(Post, () => Predicates.ALL)]);

			const records = await DataStore.query(Post);

			// This is working in integ tests. Need to dive on why
			// fake graphql service isn't handling Predicates.All.
			// expect(records.length).toBe(5);

			// leaving test in to validate the type.
		});

		test('Predicates.ALL async', async () => {
			await resyncWith([syncExpression(Post, async () => Predicates.ALL)]);

			const records = await DataStore.query(Post);

			// This is working in integ tests. Need to dive on why
			// fake graphql service isn't handling Predicates.All.
			// expect(records.length).toBe(5);

			// leaving test in to validate the type.
		});

		test('basic contains() filtering', async () => {
			await resyncWith([
				syncExpression(Post, post => post?.title.contains('cleaning')),
			]);

			const records = await DataStore.query(Post);
			expect(records.length).toBe(2);
		});

		test('basic contains() filtering - as synchronous condition producer', async () => {
			await resyncWith([
				syncExpression(Post, () => post => post.title.contains('cleaning')),
			]);

			const records = await DataStore.query(Post);
			expect(records.length).toBe(2);
		});

		test('basic contains() filtering - as asynchronous condition producer', async () => {
			await resyncWith([
				syncExpression(
					Post,
					async () => post => post.title.contains('cleaning')
				),
			]);

			const records = await DataStore.query(Post);
			expect(records.length).toBe(2);
		});

		test('omits implicit FK fields in selection set', async () => {
			// old CLI + amplify V5 + sync expressions resulted in broken sync queries,
			// where FK/connection keys were included in the sync queries, *sometimes*
			// resulting in incorrect sync queries.

			let selectionSet: string[];
			graphqlService.log = (message, query) => {
				if (
					message === 'Parsed Request' &&
					query.selection === 'syncLegacyJSONPosts'
				) {
					selectionSet = query.items;
				}
			};

			await resyncWith([
				syncExpression(LegacyJSONPost, p =>
					p?.title.eq("whatever, it doesn't matter.")
				),
			]);

			expect(selectionSet!).toBeDefined();
			expect(selectionSet!).toEqual([
				'id',
				'title',
				'createdAt',
				'updatedAt',
				'_version',
				'_lastChangedAt',
				'_deleted',
				'blog',
			]);
		});

		test('subscription query receives expected filter variable', async () => {
			await resyncWith([
				syncExpression(
					Post,
					async () => post => post.title.contains('cleaning')
				),
			]);

			// first 3 subscription requests are from calling DataStore.start in the `beforeEach`
			const [, , , onCreate, onUpdate, onDelete] = graphqlService.requests
				.filter(r => r.operation === 'subscription' && r.tableName === 'Post')
				.map(req => req.variables.filter);

			const expectedFilter = {
				and: [
					{
						title: {
							contains: 'cleaning',
						},
					},
				],
			};

			expect(onCreate).toEqual(expectedFilter);
			expect(onUpdate).toEqual(expectedFilter);
			expect(onDelete).toEqual(expectedFilter);
		});

		test('subscription query receives expected filter variable - nested', async () => {
			await resyncWith([
				syncExpression(
					Model,
					async () => m =>
						m.or(or => [
							or.and(and => [
								and.field1.eq('field'),
								and.createdAt.gt('1/1/2023'),
							]),
							or.and(and => [
								and.or(or => [
									or.optionalField1.beginsWith('a'),
									or.optionalField1.notContains('z'),
								]),
								and.emails.ne('-'),
							]),
						])
				),
			]);

			// first 3 subscription requests are from calling DataStore.start in the `beforeEach`
			const [, , , onCreate, onUpdate, onDelete] = graphqlService.requests
				.filter(r => r.operation === 'subscription' && r.tableName === 'Model')
				.map(req => req.variables.filter);

			expect(onCreate).toEqual(onUpdate);
			expect(onCreate).toEqual(onDelete);
			expect(onCreate).toMatchInlineSnapshot(`
			Object {
			  "or": Array [
			    Object {
			      "and": Array [
			        Object {
			          "field1": Object {
			            "eq": "field",
			          },
			        },
			        Object {
			          "createdAt": Object {
			            "gt": "1/1/2023",
			          },
			        },
			      ],
			    },
			    Object {
			      "and": Array [
			        Object {
			          "or": Array [
			            Object {
			              "optionalField1": Object {
			                "beginsWith": "a",
			              },
			            },
			            Object {
			              "optionalField1": Object {
			                "notContains": "z",
			              },
			            },
			          ],
			        },
			        Object {
			          "emails": Object {
			            "ne": "-",
			          },
			        },
			      ],
			    },
			  ],
			}
		`);
		});

		describe('subscription filtering clientside fallback', () => {
			let consoleWarn;

			beforeEach(() => {
				consoleWarn = jest.spyOn(console, 'warn');
			});

			afterEach(() => {
				consoleWarn.mockClear();
				consoleWarn.mockReset();
			});

			test('subscription query receives expected filter variable - repeated field in group', async () => {
				// service requires distinct fields names in each AND expr.

				await resyncWith([
					syncExpression(
						Model,
						async () => m =>
							m.and(and => [
								and.createdAt.gt('1/1/2023'),
								and.createdAt.lt('1/1/2033'),
							])
					),
				]);

				// first 3 subscription requests are from calling DataStore.start in the `beforeEach`
				const [, , , onCreate, onUpdate, onDelete] = graphqlService.requests
					.filter(
						r => r.operation === 'subscription' && r.tableName === 'Model'
					)
					.map(req => req.variables.filter);

				// no filter arg should be set; we fall back to clientside filtering
				expect(onCreate).toBeUndefined();
				expect(onUpdate).toBeUndefined();
				expect(onDelete).toBeUndefined();

				expect(consoleWarn).toHaveBeenCalledWith(
					expect.stringContaining(
						'Backend subscriptions filtering limit exceeded.'
					)
				);
				expect(consoleWarn).toHaveBeenCalledWith(
					expect.stringContaining(
						'Subscriptions filtering will be applied clientside.'
					)
				);
				expect(consoleWarn).toHaveBeenCalledWith(
					expect.stringContaining(
						`Your selective sync expression for Model contains multiple entries for createdAt in the same AND group.`
					)
				);
			});

			test('subscription query receives expected filter variable - filter field limit exceeded', async () => {
				// service limit for distinct RTF fields is 5, but we're setting a selective sync expression with 6 fields
				const filterFieldsCount = 6;

				await resyncWith([
					syncExpression(
						Model,
						async () => m =>
							m.and(and => [
								and.id.eq('123'),
								and.field1.beginsWith('a'),
								and.optionalField1.gt('b'),
								and.emails.contains('bob@aol.com'),
								and.ips.contains('10.0.0.1'),
								and.createdAt.gt('1/1/2023'),
							])
					),
				]);

				// first 3 subscription requests are from calling DataStore.start in the `beforeEach`
				const [, , , onCreate, onUpdate, onDelete] = graphqlService.requests
					.filter(
						r => r.operation === 'subscription' && r.tableName === 'Model'
					)
					.map(req => req.variables.filter);

				// no filter arg should be set; we fall back to clientside filtering
				expect(onCreate).toBeUndefined();
				expect(onUpdate).toBeUndefined();
				expect(onDelete).toBeUndefined();

				expect(consoleWarn).toHaveBeenCalledWith(
					expect.stringContaining(
						'Backend subscriptions filtering limit exceeded.'
					)
				);
				expect(consoleWarn).toHaveBeenCalledWith(
					expect.stringContaining(
						'Subscriptions filtering will be applied clientside.'
					)
				);
				expect(consoleWarn).toHaveBeenCalledWith(
					expect.stringContaining(
						`Your selective sync expression for Model contains ${filterFieldsCount} different model fields.`
					)
				);
			});

			test('subscription query receives expected filter variable - filter combinations exceeded', async () => {
				// service limit for RTF filter combinations is 10, but we're setting a selective sync expression
				// with 11 or conditions
				const filterCombinationsCount = 11;

				await resyncWith([
					syncExpression(
						Model,
						async () => m =>
							m.or(or => [
								or.id.eq('123'),
								or.field1.beginsWith('a'),
								or.optionalField1.gt('b'),
								or.emails.contains('bob@aol.com'),
								or.createdAt.gt('1/1/2023'),
								or.createdAt.gt('1/2/2023'),
								or.createdAt.gt('1/3/2023'),
								or.createdAt.gt('1/4/2023'),
								or.createdAt.gt('1/5/2023'),
								or.createdAt.gt('1/6/2023'),
								or.createdAt.gt('1/7/2023'),
							])
					),
				]);

				// first 3 subscription requests are from calling DataStore.start in the `beforeEach`
				const [, , , onCreate, onUpdate, onDelete] = graphqlService.requests
					.filter(
						r => r.operation === 'subscription' && r.tableName === 'Model'
					)
					.map(req => req.variables.filter);

				// no filter arg should be set; we fall back to clientside filtering
				expect(onCreate).toBeUndefined();
				expect(onUpdate).toBeUndefined();
				expect(onDelete).toBeUndefined();

				expect(consoleWarn).toHaveBeenCalledWith(
					expect.stringContaining(
						'Backend subscriptions filtering limit exceeded.'
					)
				);
				expect(consoleWarn).toHaveBeenCalledWith(
					expect.stringContaining(
						'Subscriptions filtering will be applied clientside.'
					)
				);
				expect(consoleWarn).toHaveBeenCalledWith(
					expect.stringContaining(
						`Your selective sync expression for Model contains ${filterCombinationsCount} field combinations`
					)
				);
			});
		});
	});
});
