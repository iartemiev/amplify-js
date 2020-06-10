import Observable, { ZenObservable } from 'zen-observable-ts';
import { ConsoleLogger as Logger } from '../Logger';

const logger = new Logger('Reachability', 'DEBUG');

type NetworkStatus = {
	online: boolean;
};

export default class ReachabilityNavigator implements Reachability {
	private static _observers: Array<
		ZenObservable.SubscriptionObserver<NetworkStatus>
	> = [];

	networkMonitor(netInfo?: any): Observable<NetworkStatus> {
		/**
		 * Here netinfo refers to @react-native-community/netinfo
		 * This is needed in React Native to enable network detection
		 * We do not import it in Core so that Apps that do not use DataStore
		 * Do not need to install and link this dependency
		 * When using Reachability in React Native, pass NetInfo as a param to networkMonitor
		 */
		if (!(netInfo && netInfo.fetch)) {
			throw new Error(
				'NetInfo must be passed to networkMonitor to enable reachability in React Native'
			);
		}
		return new Observable(observer => {
			logger.log('subscribing to reachability in React Native');

			let online = false;

			netInfo.fetch().then(({ isInternetReachable }) => {
				online = isInternetReachable;

				logger.log('Notifying initial reachability state', online);

				observer.next({ online });
			});

			const id = setInterval(async () => {
				const { isInternetReachable } = await netInfo.fetch();

				if (online !== isInternetReachable) {
					online = isInternetReachable;

					logger.log('Notifying reachability change', online);

					observer.next({ online });
				}
			}, 2000);

			ReachabilityNavigator._observers.push(observer);

			return () => {
				logger.log('unsubscribing reachability');

				clearInterval(id);

				ReachabilityNavigator._observers = ReachabilityNavigator._observers.filter(
					_observer => _observer !== observer
				);
			};
		});
	}
	// expose observers to simulate offline mode for integration testing
	private static _observerOverride(status: NetworkStatus): void {
		for (const observer of ReachabilityNavigator._observers) {
			if (observer.closed) {
				ReachabilityNavigator._observers = ReachabilityNavigator._observers.filter(
					_observer => _observer !== observer
				);
				continue;
			}
			observer.next(status);
		}
	}
}

interface Reachability {
	networkMonitor(netInfo?: any): Observable<NetworkStatus>;
}
