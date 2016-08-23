<?php
namespace elasticsearch;

class Util {
	/**
	 * Checks if passed array is associative.
	 *
	 * @param array $array
	 *
	 * @return bool
	 */
	static function is_associative( $array ) {
		$keys = array_keys( $array );
		return array_keys( $keys ) !== $keys;
	}

	/**
	 * Turn a field array containing nested dot notation fields
	 * into a hierarchical array
	 * 
	 * @param array $hierarchy the container
	 * @param array $fields to unflatten
	 *
	 * @return $hierarchy array of nested fields
	 */
	public static function apply_hierarchy( $hierarchy, $fields ) {
		foreach($fields as $field) {
			$pos = strpos($field, "."); 
			if ($pos) {
				$part = substr( $field, 0, $pos);
				$remainder = substr( $field, $pos + 1, strlen($field) - $pos);

				$container = array();
				if (array_key_exists($part, $hierarchy)) {
					$container = $hierarchy[$part];
				}

				$hierarchy[$part] = Searcher::apply_hierarchy($container, [$remainder]); 
			}
			else {
				$hierarchy[$field] = $field;
			}
		}

		return $hierarchy;
	}

}
