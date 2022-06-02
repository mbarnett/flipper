require 'flipper'

module Flipper
  module Adapters
    class MemoizingSingleKeyRedisCache
      include ::Flipper::Adapter

      ALL_FEATURE_CACHE_KEY = :flipper_singular_redis_cache_all_features

      attr_reader :name

      def initialize(adapter, client)
        @wrapped_adapter = adapter
        @client = client
        @name = :memoizing_single_key_redis_cache
      end

      def features
        get_all.keys
      end

      def get(feature)
        get_all[feature.key] || default_value
      end

      def get_multi(features)
        result = {}
        keys = features.map(&:key)
        keys.each do |key|
          result[key] = (get_all[key] || default_value)
        end
        result
      end

      def get_all
        @get_all ||= begin
          all_features = cache_value ttl: 300 do
            @wrapped_adapter.get_all
          end

          # things come out of the cache pretty much the same, except that the two arrays
          # were originally sets, and the keys in the per-feature hashes (values of the parent hash)
          # were originally symbols. So we do a quick fix-up:
          all_features.each_value do |feature_hash|
            feature_hash.symbolize_keys!
            feature_hash[:actors] = feature_hash[:actors]&.to_set
            feature_hash[:groups] = feature_hash[:groups]&.to_set
          end

          all_features
        end
      end

      # Note the ordering on the cache invalidation that follows – we want to update the underlying datastore
      # and then blow away the cached data. Invalidating the cached data first would open up a
      # race condition where some other web process would read the original underlying keys' values,
      # we would write the new data to the underlying keys, and the other web process would write the old data
      # back to the cache before the new data got read and written

      def add(feature)
        @wrapped_adapter.add(feature)
        invalidate_cache!
      end

      def remove(feature)
        @wrapped_adapter.remove(feature)
        invalidate_cache!
      end

      def clear(feature)
        @wrapped_adapter.clear(feature)
        invalidate_cache!
      end

      def enable(feature, gate, thing)
        @wrapped_adapter.enable(feature, gate, thing)
        invalidate_cache!
      end

      def disable(feature, gate, thing)
        @wrapped_adapter.disable(feature, gate, thing)
        invalidate_cache!
      end

      private

      def cache_value(ttl:, &value_lambda)
        cache = @client.get ALL_FEATURE_CACHE_KEY

        if cache.present?
          JSON.parse(cache)
        else
          cache_value = value_lambda.call
          @client.setex ALL_FEATURE_CACHE_KEY, ttl, cache_value.to_json
          cache_value
        end
      end

      def invalidate_cache!
        @client.set ALL_FEATURE_CACHE_KEY, nil
        @get_all = nil
        true
      end

      def default_value
        { boolean: nil, actors: Set.new, percentage_of_actors: nil, percentage_of_time: nil, groups: Set.new }
      end
    end
  end
end
