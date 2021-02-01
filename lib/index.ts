import RedshiftLoader from './RedshiftLoader';
import { createDefaults, mergeOptions } from './utils';
import { FactoryOptions, RSLoaderOptions } from './types';
export class RedshiftLoaderFactory {
  defaultOptions: FactoryOptions;
  constructor(options: Partial<FactoryOptions>) {
    this.defaultOptions = mergeOptions(options, createDefaults()) as FactoryOptions;
  }
  createLoader(options: RSLoaderOptions) {
    return new RedshiftLoader(mergeOptions(options, this.defaultOptions) as RSLoaderOptions);
  }
}
export { RedshiftLoader };
