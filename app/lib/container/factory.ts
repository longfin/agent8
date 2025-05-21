import type { Container, ContainerOptions } from './interfaces';
import { RemoteContainer, RemoteContainerFactory } from './remote-container-impl';
import { WebContainerFactory } from './webcontainer-impl';

/**
 * Available container types
 */
export type ContainerType = 'webcontainer' | 'remotecontainer' | 'remotecontainer-dev';

/**
 * Container factory class
 * Provides factory methods to create various container implementations
 */
export class ContainerFactory {
  private static _webContainerFactory = new WebContainerFactory();
  private static _remoteContainerFactory = new RemoteContainerFactory('agent8-controller.fly.dev', 'agent8-container');

  /**
   * Create a container of the specified type
   *
   * @param type Container type
   * @param options Container configuration options
   * @returns Created container instance
   */
  static async create(type: ContainerType, options: ContainerOptions): Promise<Container> {
    switch (type) {
      case 'webcontainer':
        return this._webContainerFactory.boot(options);
      case 'remotecontainer':
        return this._remoteContainerFactory.boot(options);
      case 'remotecontainer-dev':
        return new RemoteContainer(
          import.meta.env.VITE_REMOTE_CONTAINER_DEV_URL,
          `/home/${options.workdirName}`,
          options.v8AccessToken || '',
        );
      default:
        throw new Error(`Unknown container type: ${type}`);
    }
  }
}
