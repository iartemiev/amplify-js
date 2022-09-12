import { Storage } from '../storage/storage';
import {
	ModelInstanceMetadata,
	OpType,
	PersistentModelConstructor,
	SchemaModel,
} from '../types';
import { MutationEventOutbox } from './outbox';
import { getIdentifierValue } from './utils';
import {
	extractPrimaryKeyFieldNames,
	extractPrimaryKeysAndValues,
} from '../util';
import { ModelPredicateCreator } from '../predicates';

// https://github.com/aws-amplify/amplify-js/blob/datastore-docs/packages/datastore/docs/sync-engine.md#merger
class ModelMerger {
	constructor(
		private readonly outbox: MutationEventOutbox,
		private readonly ownSymbol: Symbol
	) {}

	public async merge<T extends ModelInstanceMetadata>(
		storage: Storage,
		model: T,
		modelDefinition: SchemaModel,
		modelConstructor: PersistentModelConstructor<T>
		// TODO: expand promise type to include no-op / null
	): Promise<OpType> {
		let result: OpType;
		const mutationsForModel = await this.outbox.getForModel(
			storage,
			model,
			modelDefinition
		);

		const isDelete = model._deleted;

		// TODO - safe to delete?
		// if (mutationsForModel.length === 0) {
		// 	if (isDelete) {
		// 		result = OpType.DELETE;
		// 		await storage.delete(model, undefined, this.ownSymbol);
		// 	} else {
		// 		[[, result]] = await storage.save(model, undefined, this.ownSymbol);
		// 	}
		// }

		// TODO - extract
		/* 
		Spec:
		FOREACH Response in Network Response
			GET local Model M WHERE identifier == Response[identifier]
			IF Response[_version] > M[_version] THEN
				WRITE Response to Storage Adapter  ## Persist latest data locally
			ELSE
				CONTINUE ## Discard Response
			END
		END
		*/

		const modelPkFields = extractPrimaryKeyFieldNames(modelDefinition);
		const identifierObject = extractPrimaryKeysAndValues(model, modelPkFields);
		const predicate = ModelPredicateCreator.createForPk<T>(
			modelDefinition,
			identifierObject
		);

		const [fromDB] = await storage.query(modelConstructor, predicate);

		if (fromDB === undefined) {
			return result;
		}

		if (fromDB._version === undefined || fromDB._version < model._version) {
			if (isDelete) {
				result = OpType.DELETE;
				await storage.delete(model, undefined, this.ownSymbol);
			} else {
				[[, result]] = await storage.save(model, undefined, this.ownSymbol);
			}
		}

		return result;
	}

	public async mergePage(
		storage: Storage,
		modelConstructor: PersistentModelConstructor<any>,
		items: ModelInstanceMetadata[],
		modelDefinition: SchemaModel
	): Promise<[ModelInstanceMetadata, OpType][]> {
		const itemsMap: Map<string, ModelInstanceMetadata> = new Map();

		for (const item of items) {
			// merge items by model id. Latest record for a given id remains.
			const modelId = getIdentifierValue(modelDefinition, item);

			itemsMap.set(modelId, item);
		}

		const page = [...itemsMap.values()];

		return await storage.batchSave(modelConstructor, page, this.ownSymbol);
	}
}

export { ModelMerger };
