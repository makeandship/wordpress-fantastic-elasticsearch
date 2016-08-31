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

	static function suggest( $text, $categories = array(), $size = 5 ) {
		$result = null;

		if (isset( $text ) && !empty( $text )) {
			$index = Indexer::_index(false);
			$search = new \Elastica\Search($index->getClient());
			$search->addIndex($index);

			$query = array( 
			    'query' => array ( 
			        'filtered' => array ( 
			            'query' => array ( 
			                'match' => array ( 
			                    'post_title_suggest' => array ( 
			                        'query' =>  strtolower($text)
			                    )
			                )
			           	)
			        )
			    ),
			    'fields' => array ( 
			    	'post_type', 
			    	'post_title', 
			    	'link'
			    )
			);
				
			if (isset($categories) && is_array($categories) && count($categories) > 0) {
				$query['query']['filtered']['filter'] = array();
				foreach($categories as $taxonomy => $filters) {
					foreach ($filters as $operation => $filter) {
						if (is_string($operation)) {
							$query['query']['filtered']['filter']['bool'] = array();

							$bool_operator = $operation === 'or' ? 'should' : 'must';
							if (!array_key_exists($bool_operator, $query['query']['filtered']['filter']['bool'])) {
								$query['query']['filtered']['filter']['bool'][$bool_operator] = array();
							}

							if (is_array($filter)) {
								foreach ($filter as $value) {
									$query['query']['filtered']['filter']['bool'][$bool_operator][] = 
										array(
											'term' => array( 
												$taxonomy => $value
											)
										);
								}
							}
						}
					}
				}
			}
			

			/*
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
			*/
			$eq = new \Elastica\Query( $query );
			$eq->setFrom( 0 );
			$eq->setSize( $size );
			$response = $search->search( $eq );

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
		$total = $response->getTotalHits();

		$hits = $response->getResults();

		if ($total > 0 && count($hits) > 0) {
			foreach($hits as $item) {
				$hit = $item->getHit();
				$fields = $hit['fields'];

				$id = $item->getId();
				$post_title = $fields['post_title'][0];
				$link = $fields['link'][0];

				$results[] = array(
					'id' => $id,
					'post_title' => $post_title,
					'link' => $link
				);
			}
		}

		return array(
			'total' => $total,
			'results' => $results
		);
	}
}