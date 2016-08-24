<?php
namespace elasticsearch;

/**
 * The searcher class provides all you need to query your ElasticSearch server.
 *
 * @license http://opensource.org/licenses/MIT
 * @author Paris Holley <mail@parisholley.com>
 * @version 4.0.1
 **/
class Searcher
{
	/**
	 * Initiate a search with the ElasticSearch server and return the results. Use Faceting to manipulate URLs.
	 * @param string $search A space delimited list of terms to search for
	 * @param integer $pageIndex The index that represents the current page
	 * @param integer $size The number of results to return per page
	 * @param array $facets An object that contains selected facets (typically the query string, ie: $_GET)
	 * @param boolean $sortByDate If false, results will be sorted by score (relevancy)
	 * @see Faceting
	 *
	 * @return array The results of the search
	 **/
	public static function search($search = '', $pageIndex = 0, $size = 10, $facets = array(), $sortByDate = false)
	{
		$args = self::_buildQuery($search, $facets);
		$query = self::_generate_query( $search, $facets );

		error_log(print_r($args, true));
		error_log('---');
		error_log(print_r($query, true));

		if (empty($args) || (empty($args['query']) && empty($args['aggs']))) {
			return array(
				'total' => 0,
				'ids' => array(),
				'facets' => array()
			);
		}

		// need to do rethink the signature of the search() method, arg list can't just keep growing
		return self::_query($query, $pageIndex, $size, $sortByDate);
	}

	public static function _generate_query($search, $facets) {
		$query = array();

		// query config
		$config = self::_get_config();

		// free text search
		// - no text 
		// - normal text 
		// - fuzzy
		// - against fields
		$query_freetext = self::_generate_query_freetext( $config, $search, $facets );

		// facets (taxonomies)
		$query_facets = self::_generate_query_facets( $config, $search, $facets );

		// ranges
		$query_ranges = self::_generate_query_ranges( $config, $search, $facets );

		// scoring
		$query_scores = self::_generate_query_scores( $config, $search, $facets );

		// taxonomy counts
		$query_aggregations = self::_generate_query_aggregations( $config, $search, $facets );

		// compose into a valid es query
		$query = self::_generate_complete_query(
			$config,
			$query_freetext,
			$query_facets,
			$query_ranges,
			$query_scores,
			$query_aggregations
			);

		return $query;
	}

	public static function _get_config() {
		return array(
			'taxonomies' => Config::taxonomies(),
			'facets' => Config::facets(),
			'numeric' => Config::option('numeric'),
			'exclude' => Config::apply_filters('searcher_query_exclude_fields', array('post_date')),
			'fields' => Config::fields(),
			'meta_fields' => Config::meta_fields()
		);
	}

	public static function _generate_query_freetext( $config, $search, $facets ) {
		$query = array(
			'query' => array()
		);

		if (isset($search) && $search) {
			$scores = self::_get_field_scores( $config );

			if (isset($scores['scored']) && !empty($scores['scored'])) {
				// - field weightings
				if (!array_key_exists('match', $query['query'])) {
					$query['query']['match'] = array();
				}

				// free text search
				$query['query']['match']['_all'] = $search;
			}
			else {
				// - normal text
				if (!array_key_exists('match', $query['query'])) {
					$query['query']['match'] = array();
				}

				// free text search
				$query['query']['match']['_all'] = $search;
			}
 
			// - fuzzy
			
		}
		else {
			// no text search
			if (!array_key_exists('match_all', $query['query'])) {
				$query['query']['match_all'] = array();
			}
		}

		return $query;
	}

	public static function _generate_query_facets( $config, $search, $facets ) {
		$query = array();

		if (isset($facets) && count($facets) > 0) {
			$query = array(
				'filter' => array()
			);

			foreach($facets as $taxonomy => $filters) {
				foreach ($filters as $operation => $filter) {
					if (is_string($operation)) {
						$query['filter']['bool'] = array();

						$bool_operator = $operation === 'or' ? 'should' : 'must';
						if (!array_key_exists($bool_operator, $query['filter']['bool'])) {
							$query['filter']['bool'][$bool_operator] = array();
						}

						if (is_array($filter)) {
							foreach ($filter as $value) {
								$query['filter']['bool'][$bool_operator][] = 
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

		return $query;
	}

	public static function _generate_query_ranges( $config, $search, $facets ) {

	}

	public static function _generate_query_scores( $config, $search, $facets ) {

	}

	public static function _generate_query_aggregations( $config, $search, $facets ) {
		$aggs = array(
			'aggs' => array()
			);

		$config_facets = $config['facets'];

		foreach ($config_facets as $facet) {
			$aggs['aggs'][$facet] = array(
				'terms' => array(
					'field' => $facet,
					'size' => Config::apply_filters('searcher_query_facet_size', 100, $facet)  // see https://github.com/elasticsearch/elasticsearch/issues/1832
				)
			);
		}

		return $aggs;
	}

	public static function _generate_complete_query( 
		$config,
		$query_freetext,
		$query_facets,
		$query_ranges,
		$query_scores,
		$query_aggregations ) {

		if (is_array($query_freetext) && !empty($query_freetext) && 
			is_array($query_facets) && !empty($query_facets) ) {
			
			$query = array(
				'query' => array( 
					'filtered' => array_merge($query_freetext, $query_facets)
				)
			);

			$query = array_merge(
				$query, 
				$query_aggregations
			);
			
		}
		else {
			$query = array_merge(
				array(),
				$query_freetext,
				$query_facets,
				$query_aggregations
			);
		}

		return $query;
	}

	public static function _get_field_scores( $config ) {
		$scores = array(
			'scored' => array(),
			'unscored' => array()
		);

		// 
		foreach($config['fields'] as $field) {
			$score = Config::score( 'field', $field);
			
			if ($score) {
				$scores['scored'][$field] = $score;
			}
			else {
				$scores['unscored'][] = $field;
			}
		}
		foreach($config['meta_fields'] as $meta_field) {
			$score = Config::score( 'meta', $field );
			
			if ($score) {
				$scores['scored'][$field] = $score;
			}
			else {
				$scores['unscored'][] = $field;
			}
		}
		foreach($config['taxonomies'] as $taxonomy) {
			$score = Config::score( 'tax', $field);
			
			if ($score) {
				$scores['scored'][$field.'_name'] = $score;
			}
			else {
				$scores['unscored'][] = $field.'_name';
			}
		}

		return $scores;
	}

	/**
	 * @internal
	 **/
	public static function _query($args, $pageIndex, $size, $sortByDate = false)
	{
		$query = new \Elastica\Query($args);
		$query->setFrom($pageIndex * $size);
		$query->setSize($size);
		//$query->setFields(array('id'));

		$query = Config::apply_filters('searcher_query', $query);

		try {
			$index = Indexer::_index(false);

			$search = new \Elastica\Search($index->getClient());
			$search->addIndex($index);

			if (!$query->hasParam('sort')) {
				if ($sortByDate) {
					$query->addSort(array('post_date' => 'desc'));
				} else {
					$query->addSort('_score');
				}
			}

			$search = Config::apply_filters('searcher_search', $search, $query);

			$results = $search->search($query);

			return self::_parseResults($results);
		} catch (\Exception $ex) {
			error_log($ex);

			Config::do_action('searcher_exception', $ex);

			return null;
		}
	}

	/**
	 * @internal
	 **/
	public static function _parseResults($response)
	{
		$val = array(
			'total' => $response->getTotalHits(),
			'facets' => array(),
			'results' => array(),
			'ids' => array()
		);

		foreach ($response->getAggregations() as $name => $agg) {
			if (isset($agg['buckets'])) {
				foreach ($agg['buckets'] as $bucket) {
					$val['facets'][$name][$bucket['key']] = $bucket['doc_count'];
				}
			}

			if (isset($agg['range']['buckets'])) {
				foreach ($agg['range']['buckets'] as $bucket) {
					$from = isset($bucket['from']) ? $bucket['from'] : '';
					$to = isset($bucket['to']) && $bucket['to'] != '*' ? $bucket['to'] : '';

					$val['facets'][$name][$from . '-' . $to] = $bucket['doc_count'];
				}
			}
		}

		foreach ($response->getResults() as $result) {
			$source = $result->getSource();
			$source['id'] = $result->getId();

			$val['results'][] = $source;
			$val['ids'][] = $source['id'];
		}

		return Config::apply_filters('searcher_results', $val, $response);
	}

	/**
	 * @internal
	 **/
	public static function _buildQuery($search, $facets = array())
	{
		global $blog_id;
		$search = str_ireplace(array(' and ', ' or '), array(' AND ', ' OR '), $search);

		$fields = array();
		$musts = array();
		$filters = array();
		$scored = array();

		// free text search
		// - no text 
		// - normal text 
		// - fuzzy
		// - against fields

		// taxonomies

		// ranges

		// scoring

		foreach (Config::taxonomies() as $tax) {
			if ($search) {
				$score = Config::score('tax', $tax);

				if ($score > 0) {
					$scored[] = "{$tax}_name^$score";
				}
			}

			self::_filterBySelectedFacets($tax, $facets, 'term', $musts, $filters);
		}

		$args = array();

		$numeric = Config::option('numeric');

		$exclude = Config::apply_filters('searcher_query_exclude_fields', array('post_date'));

		$fields = Config::fields();
		$meta_fields = Config::meta_fields();

		$search_fields = array_merge($fields, $meta_fields);
		self::_searchField(
			$search_fields, 
			'field', 
			$exclude, 
			$search, 
			$facets, 
			$musts, 
			$filters, 
			$scored, 
			$numeric);

		//self::_searchField($fields, 'field', $exclude, $search, $facets, $musts, $filters, $scored, $numeric);
		//self::_searchField(Config::meta_fields(), 'meta', $exclude, $search, $facets, $musts, $filters, $scored, $numeric);

		if ($search) {
			if (count($scored) > 0) {
				$matches = array();
				if(!preg_match('/.*?[AND|OR|:|NOT].*?/', $search, $matches)) {
					// no match
					$qs = array(
						'fields' => $scored,
						'query' => $search
					);

					$fuzzy = Config::option('fuzzy');

					if ($fuzzy && strpos($search, "~") > -1) {
						$qs['fuzzy_min_sim'] = $fuzzy;
					}

					$qs = Config::apply_filters('searcher_query_string', $qs);

					$musts[] = array('query_string' => $qs);
				}
				else {

				}
			}
			else {

			}
		}
		else {
			// apply a match all

		}

		if (in_array('post_type', $fields)) {
			self::_filterBySelectedFacets('post_type', $facets, 'term', $musts, $filters);
		}

		self::_searchField(Config::customFacets(), 'custom', $exclude, $search, $facets, $musts, $filters, $scored, $numeric);

		if (count($filters) > 0) {
			$args['filter']['bool'] = self::_filtersToBoolean($filters);
		}

		if (count($musts) > 0) {
			$args['query']['bool']['must'] = $musts;
		}

		$blogfilter = array('term' => array('blog_id' => $blog_id));

		$args['filter']['bool']['must'][] = $blogfilter;

		$args = Config::apply_filters('searcher_query_pre_facet_filter', $args);

		if (in_array('post_type', $fields)) {
			$args['aggs']['post_type']['terms'] = array(
				'field' => 'post_type',
				'size' => Config::apply_filters('searcher_query_facet_size', 100, 'post_type')  // see https://github.com/elasticsearch/elasticsearch/issues/1832
			);
		}

		// return facets
		foreach (Config::facets() as $facet) {
			$args['aggs'][$facet] = array(
				'aggregations' => array(
					"facet" => array(
						'terms' => array(
							'field' => $facet,
							'size' => Config::apply_filters('searcher_query_facet_size', 100, $facet)  // see https://github.com/elasticsearch/elasticsearch/issues/1832
						)
					)
				)
			);

			if (count($filters) > 0) {
				$applicable = array();

				foreach ($filters as $filter) {
					foreach ($filter as $type) {
						$terms = array_keys($type);

						if (!in_array($facet, $terms)) {
							// do not filter on itself when using OR
							$applicable[] = $filter;
						}
					}
				}

				if (count($applicable) > 0) {
					$args['aggs'][$facet]['filter']['bool'] = self::_filtersToBoolean($applicable);
				}
			}
		}

		if (is_array($numeric)) {
			foreach (array_keys($numeric) as $facet) {
				$ranges = Config::ranges($facet);

				if (count($ranges) > 0) {
					$args['aggs'][$facet]['aggs'] = array(
						"range" => array(
							'range' => array(
								'field' => $facet,
								'ranges' => array()
							)
						)
					);

					foreach ($ranges as $key => $range) {
						$params = array();

						if (isset($range['to'])) {
							$params['to'] = $range['to'];
						}

						if (isset($range['from'])) {
							$params['from'] = $range['from'];
						}

						$args['aggs'][$facet]['aggs']['range']['range']['ranges'][] = $params;
					}
				}
			}
		}

		if (isset($args['aggs'])) {
			foreach ($args['aggs'] as $facet => &$config) {
				if (!isset($config['filter'])) {
					$config['filter'] = array('bool' => array('must' => array()));
				}

				$config['filter']['bool']['must'][] = $blogfilter;
			}
		}

		return Config::apply_filters('searcher_query_post_facet_filter', $args);
	}

	public static function _searchField($fields, $type, $exclude, $search, $facets, &$musts, &$filters, &$scored, $numeric)
	{
		$is_scored = false;
		foreach ($fields as $field) {
			if (in_array($field, $exclude)) {
				continue;
			}

			$score = Config::score($type, $field);
			if ($score) {
				$is_scored = true;
				break;
			}
		}

		if (!$is_scored) { // phrase search when there is no score
			
			if ($search) {
				// make nested objects
				$nested_fields = Searcher::apply_hierarchy( array(), $fields );

				// generate nested field query
				$search_query = Searcher::_build_nested_search( array(), $nested_fields, $search);
			}
			else {
				// match all
			}
		} 
		else {

			foreach ($fields as $field) {
				if (in_array($field, $exclude)) {
					continue;
				}

				$score = Config::score($type, $field);
				$notanalyzed = Config::option('not_analyzed');

				if ($search && $score > 0) {
					if (strpos($search, "~") > -1 || isset($notanalyzed[$field])) {
						// TODO: fuzzy doesn't work with english analyzer
						$scored[] = "$field^$score";
					} else {
						$scored[] = sprintf(
							"$field.%s^$score",
							Config::apply_filters('string_language', 'english')
						);
					}
				}

				if (isset($numeric[$field]) && $numeric[$field]) {
					$ranges = Config::ranges($field);

					if (count($ranges) > 0) {
						$transformed = array();

						foreach ($ranges as $key => $range) {
							$transformed[$key] = array();

							if (isset($range['to'])) {
								$transformed[$key]['lt'] = $range['to'];
							}

							if (isset($range['from'])) {
								$transformed[$key]['gte'] = $range['from'];
							}
						}

						self::_filterBySelectedFacets($field, $facets, 'range', $musts, $filters, $transformed);
					}
				} else if ($type == 'custom') {
					self::_filterBySelectedFacets($field, $facets, 'term', $musts, $filters);
				}
			}
		}
	}

	public static function _build_nested_search( $container, $nested_fields, $search ) {

		foreach($nested_fields as $key => $value) {
			if (is_array($value)) {
				// ignore nested values initially
			}
			else {
				// add standard match
				if (!array_key_exists('multi_match', $container)) {
					$container['multi_match'] = array();
				}
				if (!array_key_exists('fields', $container['multi_match'])) {
					$container['multi_match']['fields'] = array();
				} 
				$container['multi_match']['fields'][] = $value;

				// TODO - move search up a level
				if (!array_key_exists('query', $container['multi_match'])) {
					$container['multi_query']['query'] = $search;
				}
			}
		}

		return $container;
	}

	public static function apply_hierarchy( $hierarchy, $fields ) {
		return Util::apply_hierarchy( $hierarchy, $fields );
	}

	public static function _filtersToBoolean($filters)
	{
		$types = array();

		$bool = array();

		foreach ($filters as $filter) {
			// is this a safe assumption?
			$type = array_keys($filter[array_keys($filter)[0]])[0];

			if (!isset($types[$type])) {
				$types[$type] = array();
			}

			$types[$type][] = $filter;
		}

		foreach ($types as $slug => $type) {
			if (count($type) == 1) {
				$bool['should'][] = $type;
			} else {
				$bool['should'][] = array('bool' => array('should' => $type));
			}
		}

		$bool['minimum_should_match'] = count($bool['should']);

		return $bool;
	}

	/**
	 * @internal
	 **/
	public static function _filterBySelectedFacets($name, $facets, $type, &$musts, &$filters, $translate = array())
	{
		if (isset($facets[$name])) {
			$facets = $facets[$name];

			if (!is_array($facets)) {
				$facets = array($facets);
			}

			foreach ($facets as $operation => $facet) {
				if (is_string($operation) && $operation == 'or') {
					// use filters so faceting isn't affecting, allowing the user to select more "or" options
					$output = &$filters;
				} else {
					$output = &$musts;
				}

				if (is_array($facet)) {
					foreach ($facet as $value) {
						$output[] = array($type => array($name => isset($translate[$value]) ? $translate[$value] : $value));
					}

					continue;
				}

				$output[] = array($type => array($name => isset($translate[$facet]) ? $translate[$facet] : $facet));
			}
		}
	}
}

?>
