export class VersionMismatchError extends Error {
  constructor() {
    super(`The item you're attempting to update has been modified by another process since it was retrieved.`);
    this.name = 'VersionMismatchError';
  }
}

export class NotFoundError extends Error {
  constructor() {
    super('Not found');
    this.name = 'NotFoundError';
  }
}
