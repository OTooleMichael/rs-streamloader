import RedshiftLoader from './RedshiftLoader';
import { mergeOptions } from './utils';
import { FactoryOptions, RSLoaderOptions, DefaultOptionInputs } from './types';
export class RedshiftLoaderFactory {
  defaultOptions: FactoryOptions;
  constructor(options: DefaultOptionInputs) {
    this.defaultOptions = mergeOptions(options) as FactoryOptions;
  }
  createLoader(options: RSLoaderOptions) {
    return new RedshiftLoader( mergeOptions<RSLoaderOptions>(options, this.defaultOptions) );
  }
}
export { RedshiftLoader };
