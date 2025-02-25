type EpochMillis = number;

export type PrimaryKey = {
  partitionKey: string | number;
  sortKey?: string | number;
};

export interface PrimaryKeyNames {
  readonly partitionKeyName: string;
  readonly sortKeyName?: string;
}

export interface Table extends PrimaryKeyNames {
  readonly tableName: string;
}

export type ConstructorItem<E extends EntityBase> = Omit<E, 'versionNumber' | 'getEntityConfig'>;

//eslint-disable-next-line @typescript-eslint/no-explicit-any
type KeyGeneratorFunction = (...args: any[]) => PrimaryKey;

export interface EntityConfig {
  table: Table;
  keyGenerator: KeyGeneratorFunction;
  indexes?: Record<string, PrimaryKeyNames>;
}

export class EntityBase {
  createdOn!: EpochMillis;
  createdBy!: string;
  updatedOn!: EpochMillis;
  updatedBy!: string;
  versionNumber!: number;

  static entityConfig: EntityConfig;

  /**
   * getEntityConfig on the derived entities will be overridden using {@link entity} decorator.
   * As long as the derived entity class has the decorator, this base method won't be executed.
   * It is defined here to enable type assistance in dynamo mapper.
   */
  getEntityConfig(): EntityConfig {
    throw new Error('getEntityConfig is not implemented on the entity');
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  constructor(_item: ConstructorItem<EntityBase>) {}
}

/**
 * Decorators provide the ability to add/modify target class members. Here we are using a decorator factory pattern
 * which returns the decorator function. decorator function implicitly gets the target class as the argument.
 * {@link https://www.typescriptlang.org/docs/handbook/decorators.html#decorator-factories}
 *
 * @param config configuration object for the entity. It holds the table, indexes, and key generator functions.
 *
 * The provided configuration is attached to the target class as a static member {@this entityConfig} and also as a
 * getter function {@this getEntityConfig}. The static member and getter functions are used in dynamo mapper to get
 * access to the entity configuration while performing Dynamo DB operations.
 */
export function entity(config: EntityConfig) {
  return <E extends typeof EntityBase>(entityClass: E): E => {
    entityClass.entityConfig = config;
    entityClass.prototype.getEntityConfig = function (): EntityConfig {
      return config;
    };

    return entityClass;
  };
}
