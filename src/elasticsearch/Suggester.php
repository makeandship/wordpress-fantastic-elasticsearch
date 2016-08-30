<?php
namespace elasticsearch;

/**
 * Returns a set of default values that are sufficient for indexing wordpress if the user does not set any values.
 *
 * @license http://opensource.org/licenses/MIT
 * @author Mark Thomsit <mark@makeandship.com>
 * @version 4.0.1
 **/
class Suggester {

	static function suggest( $text, $distance = 2 ) {
		$result = null;

		if (isset( $text ) && !empty( $text )) {
			$index = Indexer::_index(false);
			$client = $index->getClient();

			$query = array(
				'articles' => array(
					'text' => $text,
					'completion' => array(
						'field' => 'post_title_suggest',
						'fuzzy' => array(
							'edit_distance' => $distance
						)
					)
				)
			);
			$path = $index->getName() . '/_suggest';
			$response = $client->request($path, \Elastica\Request::POST, $query);

			try {
				$result = self::_parse_response( $response );
			}
			catch (\Exception $ex) {
				error_log($ex);

				Config::do_action('searcher_exception', $ex);

				return self::_empty_result();
			}
		}

		return $result;
	}

	/**
	 * Return an empty result for errors
	 */
	static function _empty_result() {
		return array();
	}

	/**
	 * Return a valid response
	 */
	static function _parse_response( $response ) {
		$results = array();

		$data = $response->getData();

		foreach( $data['articles'] as $autocomplete) {
			foreach($autocomplete['options'] as $option) {
				if (array_key_exists('text', $option)) {
					$results[] = $option['text'];
				}
			}
		}

		return $results;
	}
}